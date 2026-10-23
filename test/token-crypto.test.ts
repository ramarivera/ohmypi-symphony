import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Result } from "effect";
import * as fc from "fast-check";
import { TokenCipherError, TokenCrypto } from "../src/services/token-crypto.js";

const testKey = new Uint8Array(32).fill(0x42);
const testKeyBase64 = Buffer.from(testKey).toString("base64");

const configProvider = ConfigProvider.fromUnknown(
  Object.fromEntries(
    new Map<string, string>([
      ["LINEAR_CLIENT_ID", "test"],
      ["LINEAR_CLIENT_SECRET", "test"],
      ["LINEAR_WEBHOOK_SECRET", "test"],
      ["PUBLIC_URL", "http://localhost:3000"],
      ["TOKEN_ENCRYPTION_KEY", testKeyBase64],
    ]),
  ),
  { preserveEmptyStrings: true },
);

const TestLive = TokenCrypto.layer.pipe(
  Layer.provide(ConfigProvider.layer(configProvider)),
);

const tamper = (encoded: string): string => {
  const bytes = new Uint8Array(Buffer.from(encoded, "base64url"));
  const index = 1 + IV_BYTES;
  const value = bytes[index];
  if (value === undefined) throw new Error("Encrypted payload is truncated");
  bytes[index] = value ^ 0xff;
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

    it.effect("encrypt/decrypt round-trips all strings", () =>
      Effect.gen(function* () {
        const cases = fc.record({ plaintext: fc.string() });
        const check = ({
          plaintext,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          Effect.gen(function* () {
            const crypto = yield* TokenCrypto;
            const encoded = yield* crypto.encrypt(plaintext);
            const decoded = yield* crypto.decrypt(encoded);
            expect(decoded).toBe(plaintext);
          });
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
          ),
        );
      }),
    );

    it.effect("rejects tampered ciphertext", () =>
      Effect.gen(function* () {
        const cases = fc.record({ plaintext: fc.string() });
        const check = ({
          plaintext,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          Effect.gen(function* () {
            const crypto = yield* TokenCrypto;
            const encoded = yield* crypto.encrypt(plaintext);
            const result = yield* Effect.result(
              crypto.decrypt(tamper(encoded)),
            );
            expect(Result.isFailure(result)).toBe(true);
            if (Result.isFailure(result)) {
              expect(result.failure).toBeInstanceOf(TokenCipherError);
              expect(result.failure.reason).toBe("decrypt");
            }
          });
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
          ),
        );
      }),
    );

    it.effect("rejects unsupported version bytes", () =>
      Effect.gen(function* () {
        const cases = fc.record({
          version: fc.constantFrom(...([0, 3, 255] as const)),
        });
        const check = ({
          version,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          Effect.gen(function* () {
            const crypto = yield* TokenCrypto;
            const encoded = yield* crypto.encrypt("version check");
            const result = yield* Effect.result(
              crypto.decrypt(setVersion(encoded, version)),
            );
            expect(Result.isFailure(result)).toBe(true);
            if (Result.isFailure(result)) {
              expect(result.failure).toBeInstanceOf(TokenCipherError);
              expect(result.failure.reason).toBe("version_mismatch");
            }
          });
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
          ),
        );
      }),
    );
  });

  it.effect("fails with a bad key length", () =>
    Effect.gen(function* () {
      const badProvider = ConfigProvider.fromUnknown(
        Object.fromEntries(
          new Map<string, string>([
            ["LINEAR_CLIENT_ID", "test"],
            ["LINEAR_CLIENT_SECRET", "test"],
            ["LINEAR_WEBHOOK_SECRET", "test"],
            ["PUBLIC_URL", "http://localhost:3000"],
            ["TOKEN_ENCRYPTION_KEY", "c2hvcnQ="],
          ]),
        ),
        { preserveEmptyStrings: true },
      );
      const badLayer = TokenCrypto.layer.pipe(
        Layer.provide(ConfigProvider.layer(badProvider)),
      );
      const result = yield* Effect.result(
        TokenCrypto.use((service) => service.encrypt("x")).pipe(
          Effect.provide(badLayer),
        ),
      );
      expect(Result.isFailure(result)).toBe(true);
    }),
  );
});
