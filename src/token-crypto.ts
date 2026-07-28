const VERSION = 1;
const IV_BYTES = 12;
const TAG_BYTES = 16;

export class TokenCipher {
  readonly #key: CryptoKey;

  private constructor(key: CryptoKey) {
    this.#key = key;
  }

  static async create(rawKey: Uint8Array): Promise<TokenCipher> {
    if (rawKey.byteLength !== 32)
      throw new Error("Token cipher requires a 32-byte key");
    const keyBytes: Uint8Array<ArrayBuffer> = new Uint8Array(rawKey);
    const key = await crypto.subtle.importKey(
      "raw",
      keyBytes,
      "AES-GCM",
      false,
      ["encrypt", "decrypt"],
    );
    return new TokenCipher(key);
  }

  async encrypt(plaintext: string): Promise<string> {
    const iv: Uint8Array<ArrayBuffer> = crypto.getRandomValues(
      new Uint8Array(IV_BYTES),
    );
    const plaintextBytes: Uint8Array<ArrayBuffer> = new TextEncoder().encode(
      plaintext,
    );
    const ciphertext = new Uint8Array(
      await crypto.subtle.encrypt(
        { name: "AES-GCM", iv, tagLength: TAG_BYTES * 8 },
        this.#key,
        plaintextBytes,
      ),
    );
    const encoded = new Uint8Array(1 + iv.byteLength + ciphertext.byteLength);
    encoded[0] = VERSION;
    encoded.set(iv, 1);
    encoded.set(ciphertext, 1 + iv.byteLength);
    return Buffer.from(encoded).toString("base64url");
  }

  async decrypt(encoded: string): Promise<string> {
    const payload: Uint8Array<ArrayBuffer> = new Uint8Array(
      Buffer.from(encoded, "base64url"),
    );
    if (
      payload[0] !== VERSION ||
      payload.byteLength <= 1 + IV_BYTES + TAG_BYTES
    ) {
      throw new Error("Unsupported or truncated encrypted token");
    }
    const iv = payload.slice(1, 1 + IV_BYTES);
    const ciphertext = payload.slice(1 + IV_BYTES);
    const plaintext = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv, tagLength: TAG_BYTES * 8 },
      this.#key,
      ciphertext,
    );
    return new TextDecoder().decode(plaintext);
  }
}
