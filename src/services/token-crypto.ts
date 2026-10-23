import { Effect, Option, Redacted } from "effect";
import { TokenCipherError } from "../domain/errors.js";
import { GatewayConfig } from "./config.js";
import { SqliteClient } from "./store/sqlite-client.js";
import { migrateTokenRows } from "./token-migration.js";

export { TokenCipherError };

const VERSION = 2;
const IV_BYTES = 12;
const TAG_BYTES = 16;
const KEY_BYTES = 32;

const allocate = (length: number): Uint8Array<ArrayBuffer> =>
  new Uint8Array(length) as Uint8Array<ArrayBuffer>;

const asBufferSource = (u: Uint8Array<ArrayBufferLike>): BufferSource =>
  u as BufferSource;

const decodeKey = (
  value: string,
): Effect.Effect<Uint8Array<ArrayBuffer>, TokenCipherError> =>
  Effect.try({
    try: () => {
      const decoded = new Uint8Array(
        Buffer.from(value, "base64"),
      ) as Uint8Array<ArrayBuffer>;
      if (decoded.byteLength !== KEY_BYTES) {
        throw new TokenCipherError({
          reason: "invalid_key",
          message: `Token encryption key must be ${KEY_BYTES} bytes (got ${decoded.byteLength})`,
        });
      }
      return decoded;
    },
    catch: (error) =>
      error instanceof TokenCipherError
        ? error
        : new TokenCipherError({
            reason: "invalid_key",
            message: String(error),
          }),
  });

const importKey = (
  raw: Uint8Array<ArrayBuffer>,
): Effect.Effect<CryptoKey, TokenCipherError> =>
  Effect.tryPromise({
    try: () =>
      crypto.subtle.importKey("raw", asBufferSource(raw), "AES-GCM", false, [
        "encrypt",
        "decrypt",
      ]),
    catch: (error) =>
      new TokenCipherError({
        reason: "key_import",
        message: String(error),
      }),
  });

export class TokenCrypto extends Effect.Service<TokenCrypto>()("TokenCrypto", {
  accessors: true,
  dependencies: [GatewayConfig.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const keyId = config.tokenEncryptionKeyId ?? 1;
    const raw = yield* decodeKey(Redacted.value(config.tokenEncryptionKey));
    const key = yield* importKey(raw);
    const keys = new Map<number, CryptoKey>([[keyId, key]]);
    const previousKeys = config.tokenEncryptionPreviousKeys;
    if (previousKeys !== undefined) {
      const previous = yield* Effect.try({
        try: () => {
          const value: unknown = JSON.parse(Redacted.value(previousKeys));
          if (
            typeof value !== "object" ||
            value === null ||
            Array.isArray(value)
          )
            throw new Error("Invalid key ring");
          const entries: Array<[number, string]> = [];
          for (const [id, encoded] of Object.entries(value)) {
            if (
              !/^(?:[1-9][0-9]{0,2})$/u.test(id) ||
              Number(id) > 255 ||
              Number(id) === keyId ||
              typeof encoded !== "string"
            )
              throw new Error("Invalid key id");
            entries.push([Number(id), encoded]);
          }
          return entries;
        },
        catch: () =>
          new TokenCipherError({
            reason: "invalid_key",
            message: "Invalid previous encryption keys",
          }),
      });
      for (const [id, encoded] of previous)
        keys.set(id, yield* importKey(yield* decodeKey(encoded)));
    }

    const encrypt: (
      plaintext: string,
      context?: string,
    ) => Effect.Effect<string, TokenCipherError, never> = Effect.fn(
      "TokenCrypto.encrypt",
    )(function* (
      plaintext: string,
      context = "token",
    ): Effect.fn.Return<string, TokenCipherError, never> {
      yield* Effect.annotateCurrentSpan("plaintext.length", plaintext.length);
      const iv = crypto.getRandomValues(allocate(IV_BYTES));
      const plaintextBytes = new TextEncoder().encode(plaintext);
      const ciphertext = new Uint8Array(
        yield* Effect.tryPromise({
          try: () =>
            crypto.subtle.encrypt(
              {
                name: "AES-GCM",
                iv: asBufferSource(iv),
                additionalData: new TextEncoder().encode(context),
                tagLength: TAG_BYTES * 8,
              },
              key,
              asBufferSource(
                new Uint8Array(plaintextBytes) as Uint8Array<ArrayBuffer>,
              ),
            ),
          catch: (error) =>
            new TokenCipherError({
              reason: "encrypt",
              message: String(error),
            }),
        }),
      ) as Uint8Array<ArrayBuffer>;

      const encoded = new Uint8Array(
        2 + iv.byteLength + ciphertext.byteLength,
      ) as Uint8Array<ArrayBuffer>;
      encoded[0] = VERSION;
      encoded[1] = keyId;
      encoded.set(iv, 2);
      encoded.set(ciphertext, 2 + iv.byteLength);

      return Buffer.from(encoded).toString("base64url");
    });

    const decrypt: (
      encoded: string,
      context?: string,
    ) => Effect.Effect<string, TokenCipherError, never> = Effect.fn(
      "TokenCrypto.decrypt",
    )(function* (
      encoded: string,
      context = "token",
    ): Effect.fn.Return<string, TokenCipherError, never> {
      yield* Effect.annotateCurrentSpan("encoded.length", encoded.length);
      const payload = new Uint8Array(
        Buffer.from(encoded, "base64url"),
      ) as Uint8Array<ArrayBuffer>;

      const legacy = payload[0] === 1;
      if (payload[0] !== VERSION && !legacy) {
        return yield* Effect.fail(
          new TokenCipherError({
            reason: "version_mismatch",
            message: `Unsupported cipher version: ${payload[0]}`,
          }),
        );
      }

      const offset = legacy ? 1 : 2;
      if (payload.byteLength < offset + IV_BYTES + TAG_BYTES) {
        return yield* Effect.fail(
          new TokenCipherError({
            reason: "truncate",
            message: "Encrypted token is truncated",
          }),
        );
      }

      const iv = payload.slice(
        offset,
        offset + IV_BYTES,
      ) as Uint8Array<ArrayBuffer>;
      const ciphertext = payload.slice(
        offset + IV_BYTES,
      ) as Uint8Array<ArrayBuffer>;

      const selectedKeys = legacy
        ? [...keys.values()]
        : [keys.get(payload[1] ?? -1)].filter(
            (value): value is CryptoKey => value !== undefined,
          );
      const plaintext = new Uint8Array(
        yield* Effect.tryPromise({
          try: async () => {
            for (const selectedKey of selectedKeys) {
              try {
                return await crypto.subtle.decrypt(
                  {
                    name: "AES-GCM",
                    iv: asBufferSource(iv),
                    tagLength: TAG_BYTES * 8,
                    ...(legacy
                      ? {}
                      : { additionalData: new TextEncoder().encode(context) }),
                  },
                  selectedKey,
                  asBufferSource(ciphertext),
                );
              } catch {
                /* Old v1 rows carry no key id; try retained keys. */
              }
            }
            throw new Error("Token authentication failed");
          },
          catch: (error) =>
            new TokenCipherError({
              reason: "decrypt",
              message: String(error),
            }),
        }),
      ) as Uint8Array<ArrayBuffer>;

      return new TextDecoder().decode(asBufferSource(plaintext));
    });

    const needsReencrypt = (encoded: string): boolean => {
      const payload = Buffer.from(encoded, "base64url");
      return payload[0] !== VERSION || payload[1] !== keyId;
    };
    const sqlite = yield* Effect.serviceOption(SqliteClient);
    if (Option.isSome(sqlite))
      yield* migrateTokenRows(sqlite.value.db, {
        encrypt,
        decrypt,
        needsReencrypt,
      });
    return { encrypt, decrypt };
  }),
}) {}
