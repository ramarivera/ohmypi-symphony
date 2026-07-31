import { describe, it, expect } from "@effect/vitest";
import { ConfigProvider, Effect, Either, Layer, Schema } from "effect";
import { TokenCrypto, TokenCipherError } from "../src/services/token-crypto.js";

const testKey = new Uint8Array(32).fill(0x42);
const testKeyBase64 = Buffer.from(testKey).toString("base64");

const configProvider = ConfigProvider.fromMap(
  new Map<string, string>([["TOKEN_ENCRYPTION_KEY", testKeyBase64]]),
);

const TestLive = TokenCrypto.Default.pipe(
  Layer.provide(Layer.setConfigProvider(configProvider)),
);

const tamper = (encoded: string): string => {
  const bytes = new Uint8Array(Buffer.from(encoded, "base64url"));
  const index = 1 + IV_BYTES;
  bytes[index] = bytes[index]! ^ 0xff;
  return Buffer.from(bytes).toString("base64url");
};

const setVersion = (encoded: string, version: number): string => {
  const bytes = new Uint8Array(Buffer.from(encoded, "base64url"));
  bytes[0] = version;
  return Buffer.from(bytes).toString("base64url");
};

const IV_BYTES = 12;

describe("TokenCrypto", () => {
  it.layer(TestLive)("round-trip, tamper, and version rejection", (it) => {
    it.effect("decrypt inverts encrypt", () =>
      Effect.gen(function* () {
        const crypto = yield* TokenCrypto;
        const original = "hello, ohmypi-symphony";
        const encoded = yield* crypto.encrypt(original);
        const decoded = yield* crypto.decrypt(encoded);
        expect(decoded).toBe(original);
      }),
    );

    it.effect.prop(
      "encrypt/decrypt round-trips all strings",
      { plaintext: Schema.String },
      ({ plaintext }) =>
        Effect.gen(function* () {
          const crypto = yield* TokenCrypto;
          const encoded = yield* crypto.encrypt(plaintext);
          const decoded = yield* crypto.decrypt(encoded);
          expect(decoded).toBe(plaintext);
        }),
    );

    it.effect.prop(
      "rejects tampered ciphertext",
      { plaintext: Schema.String },
      ({ plaintext }) =>
        Effect.gen(function* () {
          const crypto = yield* TokenCrypto;
          const encoded = yield* crypto.encrypt(plaintext);
          const result = yield* Effect.either(crypto.decrypt(tamper(encoded)));
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left).toBeInstanceOf(TokenCipherError);
            expect(result.left.reason).toBe("decrypt");
          }
        }),
    );

    it.effect.prop(
      "rejects unsupported version bytes",
      { version: Schema.Literal(0, 2, 255) },
      ({ version }) =>
        Effect.gen(function* () {
          const crypto = yield* TokenCrypto;
          const encoded = yield* crypto.encrypt("version check");
          const result = yield* Effect.either(
            crypto.decrypt(setVersion(encoded, version)),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left).toBeInstanceOf(TokenCipherError);
            expect(result.left.reason).toBe("version_mismatch");
          }
        }),
    );
  });

  it.effect("fails with a bad key length", () =>
    Effect.gen(function* () {
      const badProvider = ConfigProvider.fromMap(
        new Map<string, string>([["TOKEN_ENCRYPTION_KEY", "c2hvcnQ="]]),
      );
      const badLayer = TokenCrypto.Default.pipe(
        Layer.provide(Layer.setConfigProvider(badProvider)),
      );
      const result = yield* Effect.either(
        TokenCrypto.encrypt("x").pipe(Effect.provide(badLayer)),
      );
      expect(Either.isLeft(result)).toBe(true);
    }),
  );
});
