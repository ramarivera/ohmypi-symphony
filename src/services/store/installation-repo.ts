import { Effect, Option, Schema } from "effect";
import type { AppUserId, OrganizationId, TeamId } from "../../domain/ids.js";
import { Installation as InstallationSchema, type Installation } from "../../domain/models.js";
import {
  DatabaseError,
  RowDecodeError,
  TokenCipherError,
} from "../../domain/errors.js";
import { SqliteClient, tryDb, runChanges, decodeRow, transact } from "./sqlite-client.js";
import { TokenCrypto } from "../token-crypto.js";

const InstallationRow = Schema.Struct({
  organization_id: Schema.String,
  app_user_id: Schema.String,
  access_token: Schema.String,
  refresh_token: Schema.String,
  accessible_team_ids_json: Schema.NullOr(Schema.String),
  can_access_all_public_teams: Schema.NullOr(Schema.Number),
  expires_at: Schema.Number,
  scopes_json: Schema.String,
  revoked_at: Schema.NullOr(Schema.Number),
});

type InstallationRow = Schema.Schema.Type<typeof InstallationRow>;

const parseStringArray = (
  json: string,
  label: string,
): Effect.Effect<ReadonlyArray<string>, RowDecodeError> =>
  Effect.gen(function* () {
    const value = yield* Effect.try({
      try: () => JSON.parse(json) as unknown,
      catch: (error) =>
        new RowDecodeError({
          message: `Invalid JSON in ${label}`,
          entity: "Installation",
          cause: error instanceof Error ? error.message : String(error),
        }),
    });
    if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
      return yield* Effect.fail(new RowDecodeError({
        message: `Invalid string array in ${label}`,
        entity: "Installation",
      }));
    }
    return value;
  });

const rowToInstallation = (
  tokenCrypto: TokenCrypto,
  row: InstallationRow,
): Effect.Effect<Installation, TokenCipherError | RowDecodeError, never> =>
  Effect.gen(function* () {
    const [accessToken, refreshToken] = yield* Effect.all(
      [tokenCrypto.decrypt(row.access_token), tokenCrypto.decrypt(row.refresh_token)],
      { concurrency: 2 },
    );
    const scopes = yield* parseStringArray(row.scopes_json, "installation scopes");
    let accessibleTeamIds: ReadonlyArray<string> | null = null;
    if (row.accessible_team_ids_json !== null) {
      accessibleTeamIds = yield* parseStringArray(
        row.accessible_team_ids_json,
        "installation team access",
      );
    }
    const canAccessAllPublicTeams =
      row.can_access_all_public_teams === null
        ? null
        : row.can_access_all_public_teams !== 0;
    return yield* decodeRow(
      InstallationSchema,
      {
        organizationId: row.organization_id,
        appUserId: row.app_user_id,
        accessToken,
        refreshToken,
        expiresAt: row.expires_at,
        scopes,
        revokedAt: row.revoked_at,
        accessibleTeamIds,
        canAccessAllPublicTeams,
      },
      "Installation",
    );
  });

export class InstallationRepo extends Effect.Service<InstallationRepo>()(
  "InstallationRepo",
  {
    accessors: true,
    dependencies: [TokenCrypto.Default],
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      const tokenCrypto = yield* TokenCrypto;

      const put = Effect.fn("InstallationRepo.put")(
        function* (record: Installation,) { yield* Effect.annotateCurrentSpan("organizationId", record.organizationId);
        const [accessToken, refreshToken] = yield* Effect.all(
          [tokenCrypto.encrypt(record.accessToken), tokenCrypto.encrypt(record.refreshToken)],
          { concurrency: 2 },
        );

        const accessibleTeamIds = Option.match(record.accessibleTeamIds, {
          onNone: () => null,
          onSome: (ids) => JSON.stringify(ids),
        });
        const canAccessAllPublicTeams = Option.match(
          record.canAccessAllPublicTeams,
          {
            onNone: () => null,
            onSome: (value) => (value ? 1 : 0),
          },
        );
        const revokedAt = Option.match(record.revokedAt, {
          onNone: () => null,
          onSome: (value) => value,
        });

        yield* tryDb(() =>
          db
            .query(`
              INSERT INTO installation (
                organization_id, app_user_id, access_token, refresh_token, expires_at, scopes_json, revoked_at,
                accessible_team_ids_json, can_access_all_public_teams
              ) VALUES (
                $organizationId, $appUserId, $accessToken, $refreshToken, $expiresAt, $scopes, $revokedAt,
                $accessibleTeamIds, $canAccessAllPublicTeams
              )
              ON CONFLICT(organization_id) DO UPDATE SET
                app_user_id=excluded.app_user_id, access_token=excluded.access_token,
                refresh_token=excluded.refresh_token, expires_at=excluded.expires_at,
                scopes_json=excluded.scopes_json, revoked_at=excluded.revoked_at,
                accessible_team_ids_json=excluded.accessible_team_ids_json,
                can_access_all_public_teams=excluded.can_access_all_public_teams
            `)
            .run({
              organizationId: record.organizationId,
              appUserId: record.appUserId,
              accessToken,
              refreshToken,
              expiresAt: record.expiresAt,
              scopes: JSON.stringify(record.scopes),
              revokedAt,
              accessibleTeamIds,
              canAccessAllPublicTeams,
            }), "InstallationRepo.put"); },
      );

      const get = Effect.fn("InstallationRepo.get")(
        function* (organizationId: OrganizationId,) { yield* Effect.annotateCurrentSpan("organizationId", organizationId);
        const row = yield* tryDb(
          () =>
            db
              .query<InstallationRow, [string]>(
                "SELECT * FROM installation WHERE organization_id = ?",
              )
              .get(organizationId),
          "InstallationRepo.get",
        );
        if (row === null) return Option.none();
        const decoded = yield* decodeRow(
          InstallationRow,
          row,
          "Installation",
        );
        const installation = yield* rowToInstallation(tokenCrypto, decoded);
        return Option.some(installation); },
      );

      const revoke = Effect.fn("InstallationRepo.revoke")(
        function* (organizationId: OrganizationId,
        at: number,) { yield* Effect.annotateCurrentSpan("organizationId", organizationId);
        yield* tryDb(() => {
          db.query("UPDATE installation SET revoked_at = ? WHERE organization_id = ?").run(
            at,
            organizationId,
          );
          db.query(
            "UPDATE agent_run SET desired_state='canceled', updated_at=? WHERE organization_id=? AND state NOT IN ('succeeded','failed','canceled')",
          ).run(at, organizationId);
        }, "InstallationRepo.revoke"); },
      );

      const applyPermissionChange = Effect.fn(
        "InstallationRepo.applyPermissionChange",
      )(
        function* (organizationId: OrganizationId,
        appUserId: AppUserId,
        addedTeamIds: ReadonlyArray<TeamId>,
        removedTeamIds: ReadonlyArray<TeamId>,
        canAccessAllPublicTeams: boolean,
        at: number,) { yield* Effect.annotateCurrentSpan("organizationId", organizationId);

        const tx = Effect.gen(function* () {
          const row = yield* tryDb(
            () =>
              db
              .query<{ accessible_team_ids_json: string | null }, [string, string]>(
                "SELECT accessible_team_ids_json FROM installation WHERE organization_id=? AND app_user_id=?",
              )
              .get(organizationId, appUserId),
            "InstallationRepo.applyPermissionChange.select",
          );
          if (row === null) return false;

          const teamIdsArray = yield* (row.accessible_team_ids_json === null
            ? Effect.succeed([] as Array<string>)
            : parseStringArray(
                row.accessible_team_ids_json,
                "installation team access",
              ));
          const teamIds = new Set(teamIdsArray);
          for (const teamId of addedTeamIds) teamIds.add(teamId);
          for (const teamId of removedTeamIds) teamIds.delete(teamId);

          yield* tryDb(
            () =>
              db
                .query(`
                  UPDATE installation
                  SET accessible_team_ids_json=?, can_access_all_public_teams=?
                  WHERE organization_id=? AND app_user_id=?
                `)
                .run(
                  JSON.stringify([...teamIds].sort()),
                  canAccessAllPublicTeams ? 1 : 0,
                  organizationId,
                  appUserId,
                ),
            "InstallationRepo.applyPermissionChange.update",
          );

          for (const teamId of removedTeamIds) {
            yield* tryDb(
              () =>
                db
                  .query(`
                    UPDATE agent_run SET desired_state='canceled', updated_at=?
                    WHERE organization_id=? AND team_id=? AND state NOT IN ('succeeded','failed','canceled')
                  `)
                  .run(at, organizationId, teamId),
              "InstallationRepo.applyPermissionChange.cancel",
            );
          }

          return true;
        });

        return yield* transact(db, tx); },
      );

      const getRawEncryptedAccessToken = Effect.fn(
        "InstallationRepo.getRawEncryptedAccessToken",
      )(
        function* (organizationId: OrganizationId,) { yield* Effect.annotateCurrentSpan("organizationId", organizationId);
        const row = yield* tryDb(
          () =>
            db
              .query<{ value: string }, [string]>(
                "SELECT access_token value FROM installation WHERE organization_id=?",
              )
              .get(organizationId),
          "InstallationRepo.getRawEncryptedAccessToken",
        );
        return row === null ? Option.none() : Option.some(row.value); },
      );

      const createOAuthState = Effect.fn("InstallationRepo.createOAuthState")(
        function* (hash: string,
        expiresAt: number,
        now: number,) { yield* Effect.annotateCurrentSpan("hash", hash);
        yield* tryDb(() =>
          db
            .query(
              "INSERT INTO oauth_state (state_hash, created_at, expires_at) VALUES (?, ?, ?)",
            )
            .run(hash, now, expiresAt), "InstallationRepo.createOAuthState"); },
      );

      const consumeOAuthState = Effect.fn("InstallationRepo.consumeOAuthState")(
        function* (hash: string, now: number) { yield* Effect.annotateCurrentSpan("hash", hash);
        return yield* transact(db, Effect.gen(function* () {
          const result = yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE oauth_state SET consumed_at=? WHERE state_hash=? AND consumed_at IS NULL AND expires_at>=?",
                )
                .run(now, hash, now),
            "InstallationRepo.consumeOAuthState",
          );
          return (yield* runChanges(result, "InstallationRepo.consumeOAuthState")) === 1;
        })); },
      );

      return {
        put,
        get,
        revoke,
        applyPermissionChange,
        getRawEncryptedAccessToken,
        createOAuthState,
        consumeOAuthState,
      };
    }),
  }
) {}
