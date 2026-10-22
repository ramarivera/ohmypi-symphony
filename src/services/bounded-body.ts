export class BodyTooLargeError extends Error {}

/** Enforce a streaming limit, even for chunked bodies or dishonest Content-Length. */
export async function readBoundedBody(
  input: Request | Response,
  limit: number,
): Promise<Uint8Array> {
  if (Number(input.headers.get("content-length")) > limit) {
    await input.body?.cancel();
    throw new BodyTooLargeError("Response exceeds body limit");
  }
  const reader = input.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit)
        throw new BodyTooLargeError("Response exceeds body limit");
      chunks.push(value);
    }
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    throw error;
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}
