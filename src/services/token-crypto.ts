import { Config, Effect, Redacted } from "effect";
import { TokenCipherError } from "../domain/errors.js";

export { TokenCipherError };

const VERSION = 1;
const IV_BYTES = 12;
const TAG_BYTES = 16;
const KEY_BYTES = 32;

const allocate = (length: number): Uint8Array<ArrayBuffer> =>
  new Uint8Array(length) as Uint8Array<ArrayBuffer>;

const asBufferSource = (u: Uint8Array<ArrayBufferLike>): BufferSource =>
  u as BufferSource;

const decodeKey = (value: string): Effect.Effect<Uint8Array<ArrayBuffer>, TokenCipherError> =>
  Effect.try({
    try: () => {
      const decoded = new Uint8Array(Buffer.from(value, "base64")) as Uint8Array<ArrayBuffer>;
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
      crypto.subtle.importKey(
        "raw",
        asBufferSource(raw),
        "AES-GCM",
        false,
        ["encrypt", "decrypt"],
      ),
    catch: (error) =>
      new TokenCipherError({
        reason: "key_import",
        message: String(error),
      }),
  });

export class TokenCrypto extends Effect.Service<TokenCrypto>()("TokenCrypto", {
  accessors: true,
  dependencies: [],
  effect: Effect.gen(function* () {
    const redacted = yield* Config.redacted("TOKEN_ENCRYPTION_KEY");
    const keyBase64 = Redacted.value(redacted);
    const raw = yield* decodeKey(keyBase64);
    const key = yield* importKey(raw);

    const encrypt: (plaintext: string) => Effect.Effect<string, TokenCipherError, never> =
      Effect.fn("TokenCrypto.encrypt")(
        function* (plaintext: string): Effect.fn.Return<string, TokenCipherError, never> {
          yield* Effect.annotateCurrentSpan("plaintext.length", plaintext.length);
          const iv = crypto.getRandomValues(allocate(IV_BYTES));
          const plaintextBytes = new TextEncoder().encode(plaintext);
          const ciphertext = new Uint8Array(
            yield* Effect.tryPromise({
              try: () =>
                crypto.subtle.encrypt(
                  { name: "AES-GCM", iv: asBufferSource(iv), tagLength: TAG_BYTES * 8 },
                  key,
                  asBufferSource(new Uint8Array(plaintextBytes) as Uint8Array<ArrayBuffer>),
                ),
              catch: (error) =>
                new TokenCipherError({
                  reason: "encrypt",
                  message: String(error),
                }),
            }),
          ) as Uint8Array<ArrayBuffer>;

          const encoded = new Uint8Array(
            1 + iv.byteLength + ciphertext.byteLength,
          ) as Uint8Array<ArrayBuffer>;
          encoded[0] = VERSION;
          encoded.set(iv, 1);
          encoded.set(ciphertext, 1 + iv.byteLength);

          return Buffer.from(encoded).toString("base64url");
        },
      );

    const decrypt: (encoded: string) => Effect.Effect<string, TokenCipherError, never> =
      Effect.fn("TokenCrypto.decrypt")(
        function* (encoded: string): Effect.fn.Return<string, TokenCipherError, never> {
          yield* Effect.annotateCurrentSpan("encoded.length", encoded.length);
          const payload = new Uint8Array(Buffer.from(encoded, "base64url")) as Uint8Array<ArrayBuffer>;

          if (payload[0] !== VERSION) {
            return yield* Effect.fail(
              new TokenCipherError({
                reason: "version_mismatch",
                message: `Unsupported cipher version: ${payload[0]}`,
              }),
            );
          }

          if (payload.byteLength < 1 + IV_BYTES + TAG_BYTES) {
            return yield* Effect.fail(
              new TokenCipherError({
                reason: "truncate",
                message: "Encrypted token is truncated",
              }),
            );
          }

          const iv = payload.slice(1, 1 + IV_BYTES) as Uint8Array<ArrayBuffer>;
          const ciphertext = payload.slice(1 + IV_BYTES) as Uint8Array<ArrayBuffer>;

          const plaintext = new Uint8Array(
            yield* Effect.tryPromise({
              try: () =>
                crypto.subtle.decrypt(
                  { name: "AES-GCM", iv: asBufferSource(iv), tagLength: TAG_BYTES * 8 },
                  key,
                  asBufferSource(ciphertext),
                ),
              catch: (error) =>
                new TokenCipherError({
                  reason: "decrypt",
                  message: String(error),
                }),
            }),
          ) as Uint8Array<ArrayBuffer>;

          return new TextDecoder().decode(asBufferSource(plaintext));
        },
      );

    return { encrypt, decrypt };
  }),
}) {}
