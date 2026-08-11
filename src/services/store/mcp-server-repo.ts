import { Clock, Effect, Option, Schema } from "effect";
import { DatabaseError, RowDecodeError } from "../../domain/errors.js";
import type {
  McpServerId,
  OrganizationId,
  WorkspaceId,
} from "../../domain/ids.js";
import {
  McpServerRecord,
  type McpServerRecord as McpServerRecordType,
  McpServerTransport,
  type McpServerTransport as McpServerTransportType,
} from "../../domain/models.js";
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
        url,
        env: normalizeEnv(input.env),
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

const rowToRecord = (
  row: McpServerRow,
): Effect.Effect<McpServerRecordType, RowDecodeError> =>
  Effect.gen(function* () {
    const args = yield* parseJson(row.args_json, "mcp server args");
    const env = yield* parseJson(row.env_json, "mcp server env");
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
        env: validEnv,
        repositoryId: row.repository_id,
        enabled: row.enabled === 1,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      },
      "McpServerRecord",
    );
  });

export class McpServerRepo extends Effect.Service<McpServerRepo>()(
  "McpServerRepo",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const createMcpServer = Effect.fn("McpServerRepo.createMcpServer")(
        function* (
          input: McpInput & { readonly now?: number },
        ): Effect.fn.Return<
          McpServerRecordType,
          DatabaseError | RowDecodeError
        > {
          const now = input.now ?? (yield* Clock.currentTimeMillis);
          const record = yield* validateInput(input, now);
          yield* tryDb(
            () =>
              db
                .query(
                  `INSERT INTO mcp_server
              (organization_id, id, name, transport, command, args_json, url, env_json, repository_id, enabled, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                )
                .run(
                  record.organizationId,
                  record.id,
                  record.name,
                  record.transport,
                  Option.getOrNull(record.command),
                  JSON.stringify(record.args),
                  Option.getOrNull(record.url),
                  JSON.stringify(record.env),
                  Option.getOrNull(record.repositoryId),
                  record.enabled ? 1 : 0,
                  record.createdAt,
                  record.updatedAt,
                ),
            "McpServerRepo.createMcpServer",
          );
          return record;
        },
      );

      const getMcpServer = Effect.fn("McpServerRepo.getMcpServer")(function* (
        organizationId: OrganizationId,
        id: McpServerId,
      ): Effect.fn.Return<
        Option.Option<McpServerRecordType>,
        DatabaseError | RowDecodeError
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
            yield* decodeRow(McpServerRow, row, "McpServerRecord"),
          ),
        );
      });

      const listMcpServers = Effect.fn("McpServerRepo.listMcpServers")(
        function* (
          organizationId: OrganizationId,
        ): Effect.fn.Return<
          ReadonlyArray<McpServerRecordType>,
          DatabaseError | RowDecodeError
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
          return yield* Effect.forEach(decoded, rowToRecord);
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
          DatabaseError | RowDecodeError
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
              url:
                input.url === undefined
                  ? Option.getOrNull(existing.url)
                  : input.url,
              env: input.env ?? existing.env,
              repositoryId: input.repositoryId ?? existing.repositoryId,
              enabled: input.enabled ?? existing.enabled,
            },
            now,
          );
          yield* tryDb(
            () =>
              db
                .query(
                  `UPDATE mcp_server SET name=?, transport=?, command=?, args_json=?, url=?, env_json=?, repository_id=?, enabled=?, updated_at=?
             WHERE organization_id=? AND id=?`,
                )
                .run(
                  next.name,
                  next.transport,
                  Option.getOrNull(next.command),
                  JSON.stringify(next.args),
                  Option.getOrNull(next.url),
                  JSON.stringify(next.env),
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
