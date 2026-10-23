import { createHmac, hkdfSync, randomUUID, timingSafeEqual } from "node:crypto";
import {
  Clock,
  Context,
  Effect,
  Layer,
  Option,
  Redacted,
  Schema,
} from "effect";
import { DatabaseError, type RowDecodeError } from "../../domain/errors.js";
import { DeliveryId } from "../../domain/ids.js";
import { GatewayConfig } from "../config.js";
import {
  decodeRow,
  decodeRows,
  runChanges,
  SqliteClient,
  transact,
  tryDb,
} from "./sqlite-client.js";

const DeliveryIdentityRow = Schema.Struct({
  organization_id: Schema.String,
  payload_hash: Schema.String,
  status: Schema.Literals(["pending", "processed", "failed"]),
  claim_token: Schema.NullOr(Schema.String),
});

type DeliveryIdentityRow = Schema.Schema.Type<typeof DeliveryIdentityRow>;

export type DeliveryClaimResult = "claimed" | "duplicate" | "conflict";

// Bind the normalized stored JSON to its ingress hash, tenant and format.
// payload_hash remains the hash of the original HTTP bytes for sender dedupe.
export const deliveryReplayMac = (
  secret: Redacted.Redacted<string>,
  organizationId: string,
  ingressHash: string,
  payloadJson: string,
  version: number,
): string =>
  createHmac(
    "sha256",
    Buffer.from(
      hkdfSync(
        "sha256",
        Redacted.value(secret),
        "",
        "symphony/webhook-replay/v1",
        32,
      ),
    ),
  )
    .update(JSON.stringify([version, organizationId, ingressHash, payloadJson]))
    .digest("hex");

const PendingDelivery = Schema.Struct({
  delivery_id: DeliveryId,
  organization_id: Schema.String,
  payload_hash: Schema.String,
  payload_json: Schema.String,
  payload_version: Schema.Number,
  replay_hash: Schema.NullOr(Schema.String),
});
export type PendingDelivery = Schema.Schema.Type<typeof PendingDelivery>;

export class DeliveryRepo extends Context.Service<DeliveryRepo>()(
  "DeliveryRepo",
  {
    make: Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      const { linearWebhookSecret } = yield* GatewayConfig;
      // A single gateway owns this database. Durable tokens fence writes;
      // process-local ownership distinguishes live requests from crash leftovers.
      const activeClaims = new Map<DeliveryId, string>();
      const releaseClaim = (id: DeliveryId, token: string) =>
        Effect.sync(() => {
          if (activeClaims.get(id) === token) activeClaims.delete(id);
        });
      const verifyReplay = (row: PendingDelivery): boolean => {
        if (
          row.replay_hash === null ||
          !/^[a-f0-9]{64}$/u.test(row.replay_hash)
        )
          return false;
        const expected = deliveryReplayMac(
          linearWebhookSecret,
          row.organization_id,
          row.payload_hash,
          row.payload_json,
          row.payload_version,
        );
        return timingSafeEqual(
          Buffer.from(row.replay_hash, "hex"),
          Buffer.from(expected, "hex"),
        );
      };

      const accept = Effect.fn("DeliveryRepo.accept")(function* (input: {
        readonly id: DeliveryId;
        readonly organizationId: string;
        readonly payloadHash: string;
        readonly payload: unknown;
        readonly receivedAt?: number;
        readonly claimToken?: string;
        readonly payloadVersion?: 1;
      }): Effect.fn.Return<boolean, DatabaseError> {
        yield* Effect.annotateCurrentSpan("deliveryId", input.id);
        const now = input.receivedAt ?? (yield* Clock.currentTimeMillis);
        const json = JSON.stringify(input.payload);
        const version = input.payloadVersion ?? 0;
        const result = yield* tryDb(
          () =>
            db
              .query(`
                INSERT INTO webhook_delivery (delivery_id, organization_id, received_at, payload_hash, payload_json, payload_version, replay_hash, claim_token)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(delivery_id) DO NOTHING
              `)
              .run(
                input.id,
                input.organizationId,
                now,
                input.payloadHash,
                json,
                version,
                deliveryReplayMac(
                  linearWebhookSecret,
                  input.organizationId,
                  input.payloadHash,
                  json,
                  version,
                ),
                input.claimToken ?? randomUUID(),
              ),
          "DeliveryRepo.accept",
        );
        return (yield* runChanges(result, "DeliveryRepo.accept")) === 1;
      });

      const claim = Effect.fn("DeliveryRepo.claim")(function* (input: {
        readonly id: DeliveryId;
        readonly organizationId: string;
        readonly payloadHash: string;
        readonly payload: unknown;
        readonly receivedAt?: number;
        readonly claimToken?: string;
        readonly payloadVersion?: 1;
      }): Effect.fn.Return<
        DeliveryClaimResult,
        DatabaseError | RowDecodeError
      > {
        yield* Effect.annotateCurrentSpan("deliveryId", input.id);

        const token = input.claimToken ?? randomUUID();
        const tx = Effect.gen(function* () {
          const accepted = yield* accept({ ...input, claimToken: token });
          if (accepted) return "claimed" as const;

          const existing = yield* tryDb(
            () =>
              db
                .query<DeliveryIdentityRow, [string]>(
                  "SELECT organization_id, payload_hash, status, claim_token FROM webhook_delivery WHERE delivery_id=?",
                )
                .get(input.id),
            "DeliveryRepo.claim.select",
          );
          if (existing === null) {
            return yield* Effect.fail(
              new DatabaseError({
                message: `Delivery ${input.id} disappeared`,
              }),
            );
          }
          const decoded = yield* decodeRow(
            DeliveryIdentityRow,
            existing,
            "Delivery",
          );

          if (
            decoded.payload_hash !== input.payloadHash ||
            decoded.organization_id !== input.organizationId
          ) {
            return "conflict" as const;
          }
          if (
            decoded.status === "processed" ||
            (decoded.status === "pending" &&
              decoded.claim_token !== null &&
              activeClaims.get(input.id) === decoded.claim_token)
          ) {
            return "duplicate" as const;
          }

          const result = yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE webhook_delivery SET status='pending', error=NULL, payload_json=?, payload_version=?, replay_hash=?, claim_token=? WHERE delivery_id=? AND status=? AND claim_token IS ?",
                )
                .run(
                  JSON.stringify(input.payload),
                  input.payloadVersion ?? 0,
                  deliveryReplayMac(
                    linearWebhookSecret,
                    input.organizationId,
                    input.payloadHash,
                    JSON.stringify(input.payload),
                    input.payloadVersion ?? 0,
                  ),
                  token,
                  input.id,
                  decoded.status,
                  decoded.claim_token,
                ),
            "DeliveryRepo.claim.reset",
          );
          return (result as { changes: number }).changes === 1
            ? "claimed"
            : "duplicate";
        });

        const result = yield* transact(
          db,
          tx.pipe(
            Effect.tap((result) =>
              result === "claimed"
                ? Effect.sync(() => activeClaims.set(input.id, token))
                : Effect.void,
            ),
          ),
        ).pipe(
          Effect.onError(() => releaseClaim(input.id, token)),
          Effect.uninterruptible,
        );
        return result;
      });

      const mark = Effect.fn("DeliveryRepo.mark")(function* (
        id: DeliveryId,
        status: "processed" | "failed",
        error?: string,
        claimToken?: string,
      ): Effect.fn.Return<void, DatabaseError> {
        yield* Effect.annotateCurrentSpan("deliveryId", id);
        yield* tryDb(
          () =>
            db
              .query(
                "UPDATE webhook_delivery SET status=?, error=? WHERE delivery_id=? AND status='pending' AND (? IS NULL OR claim_token=?)",
              )
              .run(
                status,
                error ?? null,
                id,
                claimToken ?? null,
                claimToken ?? null,
              ),
          "DeliveryRepo.mark",
        ).pipe(
          Effect.ensuring(
            claimToken === undefined
              ? Effect.sync(() => activeClaims.delete(id))
              : releaseClaim(id, claimToken),
          ),
        );
      });

      const pendingIds = Effect.fn("DeliveryRepo.pendingIds")(function* () {
        const rows = yield* tryDb(
          () =>
            db
              .query<{ delivery_id: string; claim_token: string | null }, []>(
                "SELECT delivery_id, claim_token FROM webhook_delivery WHERE status='pending' ORDER BY received_at, delivery_id",
              )
              .all(),
          "DeliveryRepo.pendingIds",
        );
        return yield* decodeRows(
          Schema.Struct({
            delivery_id: DeliveryId,
            claim_token: Schema.NullOr(Schema.String),
          }),
          rows,
          "Delivery",
        );
      });

      // Acquisition may fail before rotating the token. Match the original
      // token, including NULL, rather than using the unfenced mark path.
      const failReplayClaim = (id: DeliveryId, token: string | null) =>
        transact(
          db,
          Effect.gen(function* () {
            yield* tryDb(
              () =>
                db
                  .query(
                    "UPDATE webhook_delivery SET status='failed', error='Stored webhook replay failed' WHERE delivery_id=? AND status='pending' AND claim_token IS ?",
                  )
                  .run(id, token),
              "DeliveryRepo.failReplayClaim",
            );
          }),
        );

      // Rotate ownership before replay. A stale ingress/recovery fiber can no
      // longer mutate effects or mark completion after this transaction commits.
      const claimReplay = Effect.fn("DeliveryRepo.claimReplay")(function* (
        id: DeliveryId,
        token: string,
      ) {
        const claimed = yield* transact(
          db,
          Effect.gen(function* () {
            const result = yield* tryDb(
              () =>
                db
                  .query(
                    "UPDATE webhook_delivery SET claim_token=? WHERE delivery_id=? AND status='pending'",
                  )
                  .run(token, id),
              "DeliveryRepo.claimReplay",
            );
            if ((yield* runChanges(result, "DeliveryRepo.claimReplay")) === 0)
              return Option.none<PendingDelivery>();
            const row = yield* tryDb(
              () =>
                db
                  .query<PendingDelivery, [string]>(
                    "SELECT delivery_id, organization_id, payload_hash, payload_json, payload_version, replay_hash FROM webhook_delivery WHERE delivery_id=?",
                  )
                  .get(id),
              "DeliveryRepo.claimReplay.select",
            );
            const decoded = yield* decodeRow(PendingDelivery, row, "Delivery");
            activeClaims.set(id, token);
            return Option.some(decoded);
          }),
        ).pipe(
          Effect.onError(() => releaseClaim(id, token)),
          Effect.uninterruptible,
        );
        return claimed;
      });

      const processClaim = <A, E, R>(
        id: DeliveryId,
        token: string,
        effect: Effect.Effect<A, E, R>,
      ) =>
        transact(
          db,
          Effect.gen(function* () {
            const owned = yield* tryDb(
              () =>
                db
                  .query<unknown, [string, string]>(
                    "SELECT 1 FROM webhook_delivery WHERE delivery_id=? AND status='pending' AND claim_token=?",
                  )
                  .get(id, token),
              "DeliveryRepo.processClaim.fence",
            );
            if (owned === null) return false;
            yield* effect;
            yield* mark(id, "processed", undefined, token);
            return true;
          }),
        );

      const recoverPendingDeliveries = Effect.fn(
        "DeliveryRepo.recoverPendingDeliveries",
      )(function* (
        reason = "Gateway restarted before webhook processing completed",
      ): Effect.fn.Return<number, DatabaseError> {
        const result = yield* tryDb(
          () =>
            db
              .query(
                "UPDATE webhook_delivery SET status='failed', error=?, claim_token=NULL WHERE status='pending'",
              )
              .run(reason),
          "DeliveryRepo.recoverPendingDeliveries",
        );
        return yield* runChanges(
          result,
          "DeliveryRepo.recoverPendingDeliveries",
        );
      });

      return {
        accept,
        claim,
        mark,
        pendingIds,
        claimReplay,
        processClaim,
        recoverPendingDeliveries,
        releaseClaim,
        failReplayClaim,
        verifyReplay,
      };
    }),
  },
) {
  static readonly layerWithoutDependencies = Layer.effect(this, this.make);
  static readonly layer = this.layerWithoutDependencies.pipe(
    Layer.provide(GatewayConfig.layer),
  );
}
