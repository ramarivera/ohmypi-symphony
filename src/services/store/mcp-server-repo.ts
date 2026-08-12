import { Clock, Effect, Option, Schema } from "effect";
import {
  DatabaseError,
  RowDecodeError,
  type TokenCipherError,
} from "../../domain/errors.js";
import type {
  McpServerId,
  OrganizationId,
  WorkspaceId,
} from "../../domain/ids.js";
import {
  McpOAuthClientConfig,
  McpServerRecord,
  type McpServerRecord as McpServerRecordType,
  McpServerTransport,
  type McpServerTransport as McpServerTransportType,
} from "../../domain/models.js";
import { TokenCrypto } from "../token-crypto.js";
import {
  decodeRow,
  decodeRows,
  runChanges,
  SqliteClient,
  tryDb,
} from "./sqlite-client.js";

const McpServerRow = Schema.Struct({
  organization_id: Schema.String,
  id: Schema.String,
  name: Schema.String,
  transport: Schema.String,
  command: Schema.NullOr(Schema.String),
  args_json: Schema.String,
  url: Schema.NullOr(Schema.String),
  env_json: Schema.String,
  headers_json: Schema.String,
  oauth_client_json: Schema.NullOr(Schema.String),
  repository_id: Schema.NullOr(Schema.String),
  enabled: Schema.Number,
  created_at: Schema.Number,
  updated_at: Schema.Number,
});
type McpServerRow = Schema.Schema.Type<typeof McpServerRow>;

const SAFE_ID_RE = /^[a-zA-Z0-9_.-]+$/u;
const ENV_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/u;
type McpInput = {
  readonly organizationId: OrganizationId;
  readonly id: McpServerId;
  readonly name: string;
  readonly transport: McpServerTransportType;
  readonly command?: string | null;
  readonly args?: ReadonlyArray<string>;
  readonly url?: string | null;
  readonly env?: Readonly<Record<string, string>>;
  readonly headers?: Readonly<Record<string, string>>;
  readonly oauthClient?: McpOAuthClientConfig | null;
  readonly repositoryId?: Option.Option<WorkspaceId>;
  readonly enabled?: boolean;
};

const cleanString = (value: unknown, field: string): string => {
  if (typeof value !== "string") throw new Error(`${field} must be a string`);
  const result = value.trim();
  if (!result) throw new Error(`${field} must not be empty`);
  return result;
};

const normalizeArgs = (value: unknown): ReadonlyArray<string> => {
  if (!Array.isArray(value)) throw new Error("args must be an array");
  if (value.some((item) => typeof item !== "string")) {
    throw new Error("args must contain only strings");
  }
  return value.map((item) => item);
};

const normalizeEnv = (value: unknown): Readonly<Record<string, string>> => {
  if (value === undefined || value === null) return {};
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new Error("env must be an object");
  }
  const output: Record<string, string> = {};
  for (const [key, item] of Object.entries(value)) {
    if (!ENV_KEY_RE.test(key))
      throw new Error(`Invalid environment variable name: ${key}`);
    if (typeof item !== "string")
      throw new Error(`env.${key} must be a string`);
    output[key] = item;
  }
  return output;
};

const normalizeHeaders = (value: unknown): Readonly<Record<string, string>> => {
  if (value === undefined || value === null) return {};
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new Error("headers must be an object");
  }
  const output: Record<string, string> = {};
  for (const [key, item] of Object.entries(value)) {
    if (!key || /[\s\r\n]/u.test(key))
      throw new Error(`Invalid header name: ${key}`);
    if (typeof item !== "string")
      throw new Error(`headers.${key} must be a string`);
    if (/[\r\n]/u.test(item))
      throw new Error(`headers.${key} must not contain newlines`);
    output[key] = item;
  }
  return output;
};

const normalizeOAuthClient = (value: unknown): McpOAuthClientConfig | null => {
  if (value === undefined || value === null) return null;
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new Error("oauthClient must be an object or null");
  }
  const candidate = value as Record<string, unknown>;
  const clientId = cleanString(candidate.clientId, "oauthClient.clientId");
  const clientSecret =
    candidate.clientSecret === undefined || candidate.clientSecret === null
      ? undefined
      : cleanString(candidate.clientSecret, "oauthClient.clientSecret");
  const scope =
    candidate.scope === undefined || candidate.scope === null
      ? undefined
      : cleanString(candidate.scope, "oauthClient.scope");
  return {
    clientId,
    ...(clientSecret !== undefined ? { clientSecret } : {}),
    ...(scope !== undefined ? { scope } : {}),
  };
};

const validateInput = (
  input: McpInput,
  now: number,
): Effect.Effect<McpServerRecordType, RowDecodeError> =>
  Effect.try({
    try: () => {
      const organizationId = cleanString(
        input.organizationId,
        "organizationId",
      );
      const id = cleanString(input.id, "id");
      if (!SAFE_ID_RE.test(id))
        throw new Error("id must be a non-empty safe identifier");
      const name = cleanString(input.name, "name");
      const transport = Schema.decodeUnknownSync(McpServerTransport)(
        input.transport,
      );
      const command =
        input.command == null ? null : cleanString(input.command, "command");
      const url = input.url == null ? null : cleanString(input.url, "url");
      if (transport === "stdio" && command === null) {
        throw new Error("stdio transport requires command");
      }
      if (transport !== "stdio") {
        if (url === null)
          throw new Error(`${transport} transport requires url`);
        let parsed: URL;
        try {
          parsed = new URL(url);
        } catch {
          throw new Error("url must be a valid URL");
        }
        if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
          throw new Error("url must use http or https");
        }
      }
      const repositoryId = input.repositoryId ?? Option.none<WorkspaceId>();
      return Schema.decodeUnknownSync(McpServerRecord)({
        id,
        organizationId,
        name,
        transport,
        command,
        args: normalizeArgs(input.args ?? []),
        env: normalizeEnv(input.env),
        headers: normalizeHeaders(input.headers),
        oauthClient: normalizeOAuthClient(input.oauthClient),
        url,
        repositoryId: Option.match(repositoryId, {
          onNone: () => null,
          onSome: (value) => value,
        }),
        enabled: input.enabled !== false,
        createdAt: now,
        updatedAt: now,
      });
    },
    catch: (error) =>
      new RowDecodeError({
        message: error instanceof Error ? error.message : String(error),
        entity: "McpServerRecord",
        cause: String(error),
      }),
  });

const parseJson = (
  json: string,
  field: string,
): Effect.Effect<unknown, RowDecodeError> =>
  Effect.try({
    try: () => JSON.parse(json) as unknown,
    catch: (error) =>
      new RowDecodeError({
        message: `Invalid JSON in ${field}`,
        entity: "McpServerRecord",
        cause: error instanceof Error ? error.message : String(error),
      }),
  });

const MCP_ENCRYPTED_PREFIX = "mcpenc:v1:";

const isEncrypted = (value: string): boolean =>
  value.startsWith(MCP_ENCRYPTED_PREFIX);

const rowToRecord = (
  tokenCrypto: TokenCrypto,
  row: McpServerRow,
): Effect.Effect<McpServerRecordType, RowDecodeError | TokenCipherError> =>
  Effect.gen(function* () {
    const args = yield* parseJson(row.args_json, "mcp server args");
    const env = yield* parseJson(row.env_json, "mcp server env");
    const headers = yield* parseJson(row.headers_json, "mcp server headers");
    const decodedTransport = yield* decodeRow(
      McpServerTransport,
      row.transport,
      "McpServerRecord.transport",
    );
    const validArgs = yield* decodeRow(
      Schema.Array(Schema.String),
      args,
      "McpServerRecord.args",
    );
    const validEnv = yield* decodeRow(
      Schema.Record({ key: Schema.String, value: Schema.String }),
      env,
      "McpServerRecord.env",
    );
    const validHeaders = yield* decodeRow(
      Schema.Record({ key: Schema.String, value: Schema.String }),
      headers,
      "McpServerRecord.headers",
    );
    const decryptedEnv = Object.fromEntries(
      yield* Effect.forEach(Object.entries(validEnv), ([key, value]) =>
        (isEncrypted(value)
          ? tokenCrypto.decrypt(value.slice(MCP_ENCRYPTED_PREFIX.length))
          : Effect.succeed(value)
        ).pipe(Effect.map((decrypted) => [key, decrypted] as const)),
      ),
    );
    const decryptedHeaders = Object.fromEntries(
      yield* Effect.forEach(Object.entries(validHeaders), ([key, value]) =>
        (isEncrypted(value)
          ? tokenCrypto.decrypt(value.slice(MCP_ENCRYPTED_PREFIX.length))
          : Effect.succeed(value)
        ).pipe(Effect.map((decrypted) => [key, decrypted] as const)),
      ),
    );
    let oauthClient: McpOAuthClientConfig | null = null;
    if (row.oauth_client_json !== null) {
      const encoded = isEncrypted(row.oauth_client_json)
        ? yield* tokenCrypto.decrypt(
            row.oauth_client_json.slice(MCP_ENCRYPTED_PREFIX.length),
          )
        : row.oauth_client_json;
      oauthClient = yield* decodeRow(
        McpOAuthClientConfig,
        yield* parseJson(encoded, "mcp server oauth client"),
        "McpServerRecord.oauthClient",
      );
    }
    return yield* decodeRow(
      McpServerRecord,
      {
        id: row.id,
        organizationId: row.organization_id,
        name: row.name,
        transport: decodedTransport,
        command: row.command,
        args: validArgs,
        url: row.url,
        env: decryptedEnv,
        headers: decryptedHeaders,
        oauthClient,
        repositoryId: row.repository_id,
        enabled: row.enabled === 1,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      },
      "McpServerRecord",
    );
  });
const encryptEnv = (
  tokenCrypto: TokenCrypto,
  env: Readonly<Record<string, string>>,
): Effect.Effect<Record<string, string>, TokenCipherError> =>
  Effect.forEach(Object.entries(env), ([key, value]) =>
    tokenCrypto
      .encrypt(value)
      .pipe(
        Effect.map(
          (encrypted) => [key, `${MCP_ENCRYPTED_PREFIX}${encrypted}`] as const,
        ),
      ),
  ).pipe(Effect.map(Object.fromEntries));

const encryptOAuthClient = (
  tokenCrypto: TokenCrypto,
  value: McpOAuthClientConfig | null,
): Effect.Effect<string | null, TokenCipherError> =>
  value === null
    ? Effect.succeed(null)
    : tokenCrypto
        .encrypt(JSON.stringify(value))
        .pipe(Effect.map((encrypted) => `${MCP_ENCRYPTED_PREFIX}${encrypted}`));
export class McpServerRepo extends Effect.Service<McpServerRepo>()(
  "McpServerRepo",
  {
    accessors: true,
    dependencies: [TokenCrypto.Default],
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      const tokenCrypto = yield* TokenCrypto;

      const hasNameConflict = Effect.fn("McpServerRepo.hasNameConflict")(
        function* (
          organizationId: OrganizationId,
          name: string,
          repositoryId: Option.Option<WorkspaceId>,
          excludeId: string | null,
        ): Effect.fn.Return<boolean, DatabaseError> {
          const repository = Option.getOrNull(repositoryId);
          const row = yield* tryDb(
            () =>
              db
                .query<
                  { readonly id: string },
                  [string, string, string | null, string | null, string | null]
                >(
                  "SELECT id FROM mcp_server WHERE organization_id=? AND name=? AND repository_id IS ? AND (id <> ? OR ? IS NULL) LIMIT 1",
                )
                .get(organizationId, name, repository, excludeId, excludeId),
            "McpServerRepo.hasNameConflict",
          );
          return row !== null;
        },
      );
      const createMcpServer = Effect.fn("McpServerRepo.createMcpServer")(
        function* (
          input: McpInput & { readonly now?: number },
        ): Effect.fn.Return<
          McpServerRecordType,
          DatabaseError | RowDecodeError | TokenCipherError
        > {
          const now = input.now ?? (yield* Clock.currentTimeMillis);
          const record = yield* validateInput(input, now);
          if (
            yield* hasNameConflict(
              record.organizationId,
              record.name,
              record.repositoryId,
              null,
            )
          ) {
            return yield* Effect.fail(
              new DatabaseError({
                message: `MCP server name "${record.name}" already exists in this scope`,
              }),
            );
          }
          const encryptedEnv = yield* encryptEnv(tokenCrypto, record.env);
          const encryptedHeaders = yield* encryptEnv(
            tokenCrypto,
            record.headers,
          );
          const encryptedOAuthClient = yield* encryptOAuthClient(
            tokenCrypto,
            record.oauthClient ?? null,
          );
          yield* tryDb(
            () =>
              db
                .query(
                  `INSERT INTO mcp_server
              (organization_id, id, name, transport, command, args_json, url, env_json, headers_json, oauth_client_json, repository_id, enabled, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                )
                .run(
                  record.organizationId,
                  record.id,
                  record.name,
                  record.transport,
                  Option.getOrNull(record.command),
                  JSON.stringify(record.args),
                  Option.getOrNull(record.url),
                  JSON.stringify(encryptedEnv),
                  JSON.stringify(encryptedHeaders),
                  encryptedOAuthClient,
                  Option.getOrNull(record.repositoryId),
                  record.enabled ? 1 : 0,
                  record.createdAt,
                  record.updatedAt,
                ),
            "McpServerRepo.createMcpServer",
          ).pipe(
            Effect.catchTag("@Gateway/DatabaseError", (error) =>
              /unique constraint failed:\s*(?:mcp_server\.|index ['"]?mcp_server_scope_name_unique)/iu.test(
                error.message,
              )
                ? Effect.fail(
                    new DatabaseError({
                      message: `MCP server name "${record.name}" already exists in this scope`,
                      cause: error.cause,
                    }),
                  )
                : Effect.fail(error),
            ),
          );
          return record;
        },
      );

      const getMcpServer = Effect.fn("McpServerRepo.getMcpServer")(function* (
        organizationId: OrganizationId,
        id: McpServerId,
      ): Effect.fn.Return<
        Option.Option<McpServerRecordType>,
        DatabaseError | RowDecodeError | TokenCipherError
      > {
        const row = yield* tryDb(
          () =>
            db
              .query<McpServerRow, [string, string]>(
                "SELECT * FROM mcp_server WHERE organization_id=? AND id=?",
              )
              .get(organizationId, id),
          "McpServerRepo.getMcpServer",
        );
        if (row === null) return Option.none();
        return Option.some(
          yield* rowToRecord(
            tokenCrypto,
            yield* decodeRow(McpServerRow, row, "McpServerRecord"),
          ),
        );
      });

      const listMcpServers = Effect.fn("McpServerRepo.listMcpServers")(
        function* (
          organizationId: OrganizationId,
        ): Effect.fn.Return<
          ReadonlyArray<McpServerRecordType>,
          DatabaseError | RowDecodeError | TokenCipherError
        > {
          const rows = yield* tryDb(
            () =>
              db
                .query<McpServerRow, [string]>(
                  "SELECT * FROM mcp_server WHERE organization_id=? ORDER BY created_at, id",
                )
                .all(organizationId),
            "McpServerRepo.listMcpServers",
          );
          const decoded = yield* decodeRows(
            McpServerRow,
            rows,
            "McpServerRecord",
          );
          return yield* Effect.forEach(decoded, (row) =>
            rowToRecord(tokenCrypto, row),
          );
        },
      );
      const updateMcpServer = Effect.fn("McpServerRepo.updateMcpServer")(
        function* (
          organizationId: OrganizationId,
          id: McpServerId,
          input: {
            [K in keyof Omit<McpInput, "organizationId" | "id">]?:
              | Omit<McpInput, "organizationId" | "id">[K]
              | undefined;
          } & { readonly now?: number },
        ): Effect.fn.Return<
          McpServerRecordType,
          DatabaseError | RowDecodeError | TokenCipherError
        > {
          const current = yield* getMcpServer(organizationId, id);
          if (Option.isNone(current)) {
            return yield* Effect.fail(
              new DatabaseError({ message: `MCP server ${id} not found` }),
            );
          }
          const existing = current.value;
          const now = input.now ?? (yield* Clock.currentTimeMillis);
          const next = yield* validateInput(
            {
              organizationId,
              id,
              name: input.name ?? existing.name,
              transport: input.transport ?? existing.transport,
              command:
                input.command === undefined
                  ? Option.getOrNull(existing.command)
                  : input.command,
              args: input.args ?? existing.args,
              env: input.env ?? existing.env,
              headers: input.headers ?? existing.headers,
              oauthClient: input.oauthClient ?? existing.oauthClient ?? null,
              url:
                input.url === undefined
                  ? Option.getOrNull(existing.url)
                  : input.url,
              repositoryId: input.repositoryId ?? existing.repositoryId,
              enabled: input.enabled ?? existing.enabled,
            },
            now,
          );
          if (
            yield* hasNameConflict(
              organizationId,
              next.name,
              next.repositoryId,
              id,
            )
          ) {
            return yield* Effect.fail(
              new DatabaseError({
                message: `MCP server name "${next.name}" already exists in this scope`,
              }),
            );
          }
          const encryptedOAuthClient = yield* encryptOAuthClient(
            tokenCrypto,
            next.oauthClient ?? null,
          );
          const encryptedEnv = yield* encryptEnv(tokenCrypto, next.env);
          const encryptedHeaders = yield* encryptEnv(tokenCrypto, next.headers);
          yield* tryDb(
            () =>
              db
                .query(
                  `UPDATE mcp_server SET name=?, transport=?, command=?, args_json=?, url=?, env_json=?, headers_json=?, oauth_client_json=?, repository_id=?, enabled=?, updated_at=?
             WHERE organization_id=? AND id=?`,
                )
                .run(
                  next.name,
                  next.transport,
                  Option.getOrNull(next.command),
                  JSON.stringify(next.args),
                  Option.getOrNull(next.url),
                  JSON.stringify(encryptedEnv),
                  JSON.stringify(encryptedHeaders),
                  encryptedOAuthClient,
                  Option.getOrNull(next.repositoryId),
                  next.enabled ? 1 : 0,
                  now,
                  organizationId,
                  id,
                ),
            "McpServerRepo.updateMcpServer",
          );
          return { ...next, createdAt: existing.createdAt, updatedAt: now };
        },
      );

      const deleteMcpServer = Effect.fn("McpServerRepo.deleteMcpServer")(
        function* (
          organizationId: OrganizationId,
          id: McpServerId,
        ): Effect.fn.Return<boolean, DatabaseError> {
          const result = yield* tryDb(
            () =>
              db
                .query(
                  "DELETE FROM mcp_server WHERE organization_id=? AND id=?",
                )
                .run(organizationId, id),
            "McpServerRepo.deleteMcpServer",
          );
          return (
            (yield* runChanges(result, "McpServerRepo.deleteMcpServer")) === 1
          );
        },
      );

      return {
        createMcpServer,
        getMcpServer,
        listMcpServers,
        updateMcpServer,
        deleteMcpServer,
      };
    }),
  },
) {}
