import { lookup } from "node:dns/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { BlockList, isIP } from "node:net";
import { Readable } from "node:stream";
import { readBoundedBody } from "./bounded-body.js";

const nativeFetch = globalThis.fetch;
const blocked = new BlockList();
for (const [address, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const)
  blocked.addSubnet(address, prefix, "ipv4");
for (const [address, prefix] of [
  ["2001::", 32],
  ["2001:db8::", 32],
  ["2002::", 16],
] as const)
  blocked.addSubnet(address, prefix, "ipv6");

export const isPublicAddress = (address: string): boolean => {
  const family = isIP(address);
  if (family === 4) return !blocked.check(address, "ipv4");
  // Only global unicast. This excludes mapped IPv4, ULA, local, multicast,
  // NAT64 and link-local addresses, including cloud metadata IPv6 addresses.
  return (
    family === 6 && /^[23]/u.test(address) && !blocked.check(address, "ipv6")
  );
};
export const isLoopbackAddress = (address: string): boolean =>
  address === "::1" || (isIP(address) === 4 && address.startsWith("127."));
export interface NetworkPolicy {
  readonly allowLoopbackHttp?: boolean | undefined;
}
export const MCP_MAX_RESPONSE_BYTES = 1024 * 1024;

/** Resolve once and pin the actual socket lookup to that vetted address. */
const pinnedRequest = (
  url: URL,
  init: RequestInit,
  address: string,
  family: number,
): Promise<Response> =>
  new Promise((resolve, reject) => {
    const transport = url.protocol === "https:" ? httpsRequest : httpRequest;
    const headers = Object.fromEntries(new Headers(init.headers));
    const req = transport(
      url,
      {
        method: init.method ?? "GET",
        headers,
        signal: init.signal ?? undefined,
        agent: false,
        family,
        lookup: (_hostname, _options, callback) =>
          callback(null, address, family),
      },
      (res) => {
        try {
          const status = res.statusCode ?? 502;
          if (status < 200 || status > 599)
            throw new Error("Invalid OAuth HTTP response status");
          const responseHeaders = new Headers();
          for (const [key, value] of Object.entries(res.headers)) {
            if (value !== undefined)
              responseHeaders.set(
                key,
                Array.isArray(value) ? value.join(", ") : value,
              );
          }
          resolve(
            new Response(
              [204, 205, 304].includes(status)
                ? null
                : (Readable.toWeb(
                    res,
                  ) as unknown as ReadableStream<Uint8Array>),
              { status, headers: responseHeaders },
            ),
          );
          if ([204, 205, 304].includes(status)) res.resume();
        } catch (error) {
          res.destroy();
          reject(error);
        }
      },
    );
    req.on("error", reject);
    // These endpoints only send form/JSON strings. Never forward arbitrary bodies.
    if (typeof init.body === "string") req.write(init.body);
    req.end();
  });

export async function safeOAuthFetch(
  input: string | URL | Request,
  init: RequestInit = {},
  policy: NetworkPolicy = {},
  transport = nativeFetch,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    let url = new URL(input instanceof Request ? input.url : input);
    for (let hop = 0; hop <= 3; hop++) {
      if (
        url.username ||
        url.password ||
        !["https:", "http:"].includes(url.protocol)
      )
        throw new Error("Untrusted OAuth endpoint");
      const hostname = url.hostname.replace(/^\[|\]$/gu, "");
      const addresses = isIP(hostname)
        ? [{ address: hostname, family: isIP(hostname) }]
        : await Promise.race([
            lookup(hostname, { all: true, verbatim: true }),
            new Promise<never>((_resolve, reject) =>
              controller.signal.addEventListener(
                "abort",
                () => reject(new Error("OAuth DNS lookup timed out")),
                { once: true },
              ),
            ),
          ]);
      if (controller.signal.aborted) throw new Error("OAuth request timed out");
      const loopback =
        policy.allowLoopbackHttp === true &&
        url.protocol === "http:" &&
        addresses.length > 0 &&
        addresses.every(({ address }) => isLoopbackAddress(address));
      if (url.protocol !== "https:" && !loopback)
        throw new Error("OAuth endpoint must use HTTPS");
      if (
        !addresses.length ||
        (!loopback &&
          addresses.some(({ address }) => !isPublicAddress(address)))
      )
        throw new Error("OAuth endpoint resolves to a forbidden address");
      const chosen = addresses[0];
      if (!chosen) throw new Error("OAuth endpoint did not resolve");
      const options = {
        ...init,
        signal: controller.signal,
        redirect: "manual" as const,
      };
      const response =
        transport === nativeFetch
          ? await pinnedRequest(url, options, chosen.address, chosen.family)
          : await transport(url, options);
      if ([301, 302, 303, 307, 308].includes(response.status)) {
        await response.body?.cancel();
        const location = response.headers.get("location");
        if (!location || hop === 3)
          throw new Error("OAuth redirect limit reached");
        const next = new URL(location, url);
        // Never replay credentials or POST bodies at another endpoint.
        if (next.origin !== url.origin || (init.method ?? "GET") !== "GET")
          throw new Error("Untrusted OAuth redirect");
        url = next;
        continue;
      }
      const body = await readBoundedBody(response, MCP_MAX_RESPONSE_BYTES);
      return new Response(
        [204, 205, 304].includes(response.status) ? null : new Uint8Array(body),
        {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers,
        },
      );
    }
    throw new Error("OAuth redirect limit reached");
  } finally {
    clearTimeout(timeout);
  }
}
