# T3 Code sidecar for the gateway's T3 driver

The gateway's T3 driver doesn't run agents itself. It talks to a T3 Code
server (`t3 serve`) in a sidecar container, and T3 runs the Codex and Claude
Code CLIs. Both CLIs send model traffic to CLIProxyAPI, configured the same
way as the `claude-cliproxy` and `codex-cliproxy` provider instances in
Ramiro's workbench T3 install.

Files:

| Path | What it is |
| --- | --- |
| `deploy/t3/Dockerfile` | Image with pinned, checksum-verified `t3`, `codex`, and `claude` |
| `deploy/t3/docker-entrypoint.sh` | Renders T3 settings and the Codex config from env, then runs `t3 serve` |
| `deploy/t3/issue-gateway-token.sh` | One-shot auth bootstrap that mints the gateway's bearer token |

Tracking: bead `sym-gmy.2` (this sidecar), `sym-gmy.4` (toolbox stack wiring),
epic `sym-gmy`.

## Architecture

```text
            private stack network (not edge-proxy-published)
 gateway ──────────── HTTP + WS RPC :3773 ──────────────▶ t3 sidecar
 (bun, uid 1000)       Bearer → /api/auth/websocket-ticket   t3 serve (uid 1000)
   │                   → /ws?wsTicket=…&orchestrationProtocol=2   ├─ codex app-server ─┐
   │                                                              └─ claude (SDK)    ─┤
   │  /app/workspaces  ◀──── shared volume, same path ────▶  /app/workspaces          │
   │  /run/secrets/t3-gateway-token ◀── t3-gateway-auth volume (ro)                   │
   ▼                                                                                  ▼
 Linear                                               https://cliproxyapi.ai.roxasroot.net
                                                       (/v1 for Codex, root for Claude)
```

- The gateway uses the WebSocket RPC surface (protocol 2): `orchestration.launchThread`,
  `orchestration.dispatchCommand`, `orchestration.subscribeThread`,
  `orchestration.getThreadProjection`, `server.getConfig`, and `projects.*`. A token
  with `orchestration:read` + `orchestration:operate` covers all of these, including
  project creation without project scripts.
- `/mcp` is a separate audience. T3 only accepts `mcp-client` sessions there, so the
  gateway token gets a 401 on `/mcp`. That's expected.
- Workspaces live on a volume mounted at `/app/workspaces` in both containers. T3
  projects are registered by path, so the path has to match. Both containers run as
  uid/gid 1000 (the gateway image's `bun` user and this image's `t3` user), so files
  either side creates stay writable by the other.

## Image contents and pins

| Component | Pinned version | Source | Integrity check |
| --- | --- | --- | --- |
| `t3` server | 0.1.2 | GitHub release `pingdotgg/t3code` `v0.1.2`, asset `t3-0.1.2-linux-{x64,arm64}.tar.gz` | SHA-256 from the release's `SHA256SUMS` |
| `codex` | 0.156.1 | GitHub release `openai/codex` `rust-v0.156.1`, `codex-package-*-unknown-linux-musl.tar.gz` | SHA-256 that T3 0.1.2 itself pins in `apps/server/src/provider/CodexInstallation.ts` |
| `claude` | 2.1.304 | Anthropic's release bucket, `claude-code-releases/2.1.304/linux-{x64,arm64}/claude` | SHA-256 from that release's `manifest.json` |

Why these versions:

- **t3 0.1.2 stable, not 0.1.2-nightly.20261021.3473.** arch-cachy's host service runs
  that nightly. The stable 0.1.2 tag was cut a day later from the same 0.1.2 line, has
  a `SHA256SUMS` asset, is the npm `latest` dist-tag, and won't be pruned the way old
  nightlies are. It has everything the driver needs: WS protocol 2,
  `t3 auth session issue --scope`, and `t3 thread`.
- **codex 0.156.1.** It's the build T3 0.1.2 installs when it manages Codex itself, so
  it's the version this T3 release was tested against. arch-cachy has 0.159.0 and
  workbench has 0.163.0; neither is needed for T3 0.1.2.
- **claude 2.1.304.** The same build that runs on workbench and arch-cachy today.

The base image is `debian:trixie-slim` (glibc; the `t3` binary isn't musl-compatible).
It adds `libatomic1` (the one extra library `t3` links against), `git`,
`openssh-client`, `ripgrep`, and `procps` for the agents' tool calls, `jq` for config
rendering, and `tini` as PID 1 to reap agent subprocesses. No secrets are in the image.

## Environment variables

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `CLIPROXYAPI_BASE_URL` | yes | none | CLIProxyAPI origin, e.g. `https://cliproxyapi.ai.roxasroot.net`. Claude uses it as is; Codex gets `<url>/v1` |
| `CLIPROXYAPI_API_KEY` or `CLIPROXYAPI_API_KEY_FILE` | one of them | none | CLIProxyAPI client bearer. Setting both is an error |
| `T3_CODEX_MODEL` | no | `gpt-6-sol` | Codex default model in the generated `config.toml` |
| `T3_CODEX_REASONING_EFFORT` | no | `medium` | Codex `model_reasoning_effort` |
| `T3_CODEX_CONFIG_EXTRA_FILE` | no | none | TOML appended to the generated Codex config, e.g. `[mcp_servers.*]` for the gateway's Linear host tools |
| `T3_DEFAULT_PROVIDER_INSTANCE` | no | `claude-cliproxy` | T3 `defaultModelSelection.instanceId` |
| `T3_DEFAULT_MODEL` | no | `claude-opus-5-5` | T3 `defaultModelSelection.model` |
| `T3_CODEX_HOME` | no | `$T3CODE_HOME/codex-cliproxy` | `CODEX_HOME` for the codex-cliproxy instance |
| `T3_CLAUDE_CONFIG_DIR` | no | `$T3CODE_HOME/claude-cliproxy` | `CLAUDE_CONFIG_DIR` for the claude-cliproxy instance |
| `T3_WORKSPACE_ROOT` | no | `/app/workspaces` | Working directory passed to `t3 serve` |
| `T3CODE_HOME` | set in image | `/data/t3` | T3 data dir (SQLite DBs, settings, secrets) |
| `T3CODE_HOST` / `T3CODE_PORT` | set in image | `0.0.0.0` / `3773` | Listen address. Publish only on a private network |
| `T3_GATEWAY_TOKEN_FILE` | no | `/data/gateway-auth/t3-gateway-token` | Where `t3-issue-gateway-token` writes |

The CLIProxyAPI bearer is a placeholder today: `cliproxyapi.yaml` sets `api-keys: []`,
and the owner-only Tailscale ACL `tag:svc-cliproxyapi` is the real auth boundary (see
the header of the toolbox's `home/dot_codex-cliproxy/modify_private_config.toml.nu.tmpl`).
Treat it as a secret anyway and pass it through `_FILE`, so nothing changes if
CLIProxyAPI starts enforcing keys.

## What the entrypoint renders

On every start, before `t3 serve`:

1. **`$T3_CODEX_HOME/config.toml`** (0600), fully regenerated. It matches workbench's
   `~/.codex-cliproxy/config.toml`: `model_provider = "cliproxyapi"`,
   `approval_policy = "never"`, `sandbox_mode = "danger-full-access"`, and
   `[model_providers.cliproxyapi]` with `base_url = "<CLIPROXYAPI_BASE_URL>/v1"` and
   `wire_api = "responses"`. One deliberate difference: it uses
   `env_key = "CLIPROXYAPI_API_KEY"` instead of an inline `experimental_bearer_token`,
   so the key stays off disk. The entrypoint exports `CLIPROXYAPI_API_KEY`, and every
   agent T3 spawns inherits it. Codex appends `[projects."…"] trust_level` entries at
   runtime; a restart drops them, which doesn't matter with `approval_policy = "never"`.
   Workbench's Remnic plugin and marketplace entries are left out.
2. **`$T3CODE_HOME/userdata/settings.json`** (0600), merged with `jq`. Managed keys get
   overwritten; everything else (UI changes, other instances) is kept:
   - `providerInstances["claude-cliproxy"]`: driver `claudeAgent`, environment
     `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN` (marked `sensitive`, so T3 redacts it
     in client responses), and `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1` (as in
     toolbox's `claudex` wrapper). `config.homePath` is set to a dir on the data volume
     so Claude session transcripts survive restarts; workbench leaves it at `~/.claude`.
   - `providerInstances["codex-cliproxy"]`: driver `codex`, `config.homePath` set to
     `$T3_CODEX_HOME`.
   - `providerInstances.codex` and `.claudeAgent` (T3's built-in instances) are set to
     `enabled: false` if settings.json doesn't mention them yet. They have no login in
     the container and would only report errors.
   - `defaultModelSelection` from `T3_DEFAULT_PROVIDER_INSTANCE` / `T3_DEFAULT_MODEL`.

`ANTHROPIC_API_KEY` is unset before T3 starts, so a stray key can't route Claude
around the proxy.

Container commands: `serve [flags]` (default) renders and runs `t3 serve --no-browser`;
`render` only renders and exits; anything else runs as given (`bash`, `t3 doctor`, …).

## Volumes

| Mount | Contents | Notes |
| --- | --- | --- |
| `/data/t3` | `T3CODE_HOME`: `userdata/state*.sqlite`, `settings.json`, T3 `secrets/`, `codex-cliproxy/`, `claude-cliproxy/` | Persistent. Grows with thread history (workbench's `statev2.sqlite` is 1.47 GB), so back it up and watch disk |
| `/data/gateway-auth` | `t3-gateway-token` (0400) and `t3-gateway-token.json` (session id, expiry, no token) | Mount read-only into the gateway |
| `/app/workspaces` | Shared with the gateway, same path | Must be git repos or worktrees for T3 projects |

## Auth bootstrap

`t3 serve` prints a startup pairing token, URL, and QR code to the container log.
That credential is one-time, expires after 5 minutes, and grants the standard
client scopes (`issueStartupPairingCredential` in `EnvironmentAuth.ts`). `t3 start`
announces a pairing URL too, so there's no flag to avoid it. Reading `docker logs`
needs Docker access, which is root-equivalent anyway; the real exposure is a log
shipper that forwards container stdout somewhere wider. If the stack ships logs,
filter `Token:` and `Pairing URL:` lines from this container. To pair a browser later, run `t3 auth pairing create` in the container.
To clear an unused one, run `t3 auth pairing list` and `t3 auth pairing revoke <id>`.

The gateway gets a scoped bearer session instead:

```bash
docker compose exec t3 t3-issue-gateway-token            # 30-day TTL
docker compose exec t3 t3-issue-gateway-token --ttl 7d   # shorter
```

The script runs
`t3 auth session issue --scope orchestration:read --scope orchestration:operate --ttl <ttl> --label "ohmypi-symphony gateway" --json`
against the server's data dir. It writes only the token to
`/data/gateway-auth/t3-gateway-token` (mode 0400, uid 1000, atomic rename) and the
non-secret metadata to `t3-gateway-token.json`. It works while the server is running,
because it talks to the same SQLite auth store.

The gateway reads the token as a `_FILE` secret, POSTs it as a bearer to
`/api/auth/websocket-ticket`, and connects to
`/ws?wsTicket=<ticket>&orchestrationProtocol=2`.

Rotation: run the script again, restart the gateway, then revoke the old session.
The script prints the exact `t3 auth session revoke <id>` command. Sessions expire
at the TTL and there's no refresh, so schedule rotation before expiry (lane C2).

## Agents and systemd-run in a container

T3 on Linux tries to start each agent in a transient `systemd-run --user --scope`
unit (`apps/server/src/process/agentScope.ts`). It does that only when
`XDG_RUNTIME_DIR` is set and both `systemd-run` and `systemctl` resolve on `PATH`,
and when a probe scope starts successfully. Otherwise `AgentScope.wrap` falls back
to `/bin/sh -c '<raise oom_score_adj>; exec "$@"'`, a plain child process.

The image has no systemd and doesn't set `XDG_RUNTIME_DIR`, so agents run as direct
children of `t3`. That's verified: the process tree shows `codex app-server` and
`claude --output-format stream-json …` under `t3 serve`, and both answered prompts.
What's lost compared with the host install:

- No per-agent cgroup memory limits (`MemoryHigh`/`MemoryMax` on the agents slice) and
  no OOM attribution per thread. Set a container-level `mem_limit` instead.
- No cleanup of leftover agents from a killed server by slice. Container restarts
  kill every process anyway.

Don't install systemd or mount the host's user bus to get this back.

## Verification (2026-10-23, workbench, image `ohmypi-symphony-t3:dev`)

Evidence lives in `~/dev/agentic-scratchpads/ohmypi-symphony/t3-sidecar/`:

- `build.log`: image builds and every checksum passes.
- `versions.log`: `t3 v0.1.2`, `codex-cli 0.156.1`, `2.1.304 (Claude Code)`; uid 1000;
  no `systemd-run`.
- Container became `healthy` (healthcheck: `GET /.well-known/t3/environment`, which
  reports `serverVersion 0.1.2`, `orchestrationProtocolVersion 2`).
- `token-issue.log`: token written with mode 0400; scopes `orchestration:read`,
  `orchestration:operate`; 30-day expiry.
- `getconfig.json`, `providers-after-restart.log`: `server.getConfig` over WS with the
  gateway token. `claude-cliproxy` is `ready` (auth `authenticated`, apiKey) and
  `codex-cliproxy` is `ready`, with its model list (`gpt-6-sol`, `gpt-6-luna`, …)
  fetched through CLIProxyAPI. After a restart on the same volume, only those two
  instances are enabled and the token still works.
- `probe-prompts.log`: `t3 thread start … "Reply with exactly PROBE-OK" --wait` per
  provider. Both returned `PROBE-OK` (codex-cliproxy/gpt-6-sol and
  claude-cliproxy/claude-opus-5-5).
- `ws-launch.json`, `ws-projection.json`: `orchestration.launchThread` with the gateway
  token (`runtimeMode: "full-access"`, codex-cliproxy) completed with `PROBE-OK`.
- `mcp-reject.log`: the gateway token gets 401 on `/mcp`, as designed.
- `negative-tests.log`: missing URL, missing key, and both key forms set each fail
  fast with exit 64.

Smoke run:

```bash
docker build -f deploy/t3/Dockerfile -t ohmypi-symphony-t3:dev deploy/t3
docker run -d --name t3-sidecar-smoke -p 127.0.0.1:37730:3773 \
  -v t3-sidecar-smoke-data:/data/t3 -v t3-sidecar-smoke-auth:/data/gateway-auth \
  -v "$PWD/cliproxy-key:/run/secrets/cliproxyapi-key:ro" \
  -e CLIPROXYAPI_BASE_URL=https://cliproxyapi.ai.roxasroot.net \
  -e CLIPROXYAPI_API_KEY_FILE=/run/secrets/cliproxyapi-key \
  ohmypi-symphony-t3:dev
docker exec t3-sidecar-smoke t3-issue-gateway-token
```

## Upgrade and pin procedure

Bump one component at a time, in `deploy/t3/Dockerfile`:

1. **t3.** Pick a stable tag (`gh release list -R pingdotgg/t3code`). Take both Linux
   checksums from the tag's `SHA256SUMS`
   (`gh release download v<X> -R pingdotgg/t3code -p SHA256SUMS -O -`). Update
   `T3_VERSION` and both `t3_sum` values. Then read the T3 diff for
   `packages/contracts/src/orchestrationV2.ts`, `rpc.ts`, and
   `apps/server/src/auth/RpcAuthorization.ts`. The WS contract is internal and
   unversioned, so the gateway driver's vendored schema has to move with it.
2. **codex.** Prefer the version the new T3 tag pins in
   `apps/server/src/provider/CodexInstallation.ts`, and copy its Linux musl checksums.
3. **claude.** Read
   `https://storage.googleapis.com/claude-code-dist-86c565f3-f756-42ad-8dfa-d59b1c096819/claude-code-releases/<version>/manifest.json`
   and copy `platforms["linux-x64"].checksum` and `["linux-arm64"].checksum`.
4. Rebuild, rerun the smoke steps above (health, token, `server.getConfig`, one
   PROBE-OK per provider), and update the pin table in this doc.

`t3 update` and Claude's autoupdater must not run in the container
(`DISABLE_AUTOUPDATER=1` is set). The image is the pin.

## What lane C2 adds to the toolbox stack (bead `sym-gmy.4`)

The ohmypi-symphony compose stack on arch-cachy (`home/stacks/…`, registered in
`home/.chezmoidata/stacks.yaml`) needs:

1. **A `t3` service** built from this repo's `deploy/t3/Dockerfile` with context
   `deploy/t3`, or from a published image tag. Settings:
   - `restart: unless-stopped`, `init` not needed (tini is PID 1), and a `mem_limit`
     (no per-agent cgroups, see above).
   - Networks: a private stack network shared with the gateway. Don't publish 3773
     on the host or route it through the edge proxy. The container must resolve and
     reach `cliproxyapi.ai.roxasroot.net` over Tailscale, the same way the gateway
     container already does on `edge-proxy`.
2. **Environment:** `CLIPROXYAPI_BASE_URL=https://cliproxyapi.ai.<root_domain>`
   (resolve it from `roxasroot_net_edge.vpn_services`, as the codex template does) and
   `CLIPROXYAPI_API_KEY_FILE` pointing at a mounted secret. Keep the placeholder's
   source of truth where it lives today, and don't commit it to fnox or the stack dir
   in plaintext.
3. **Volumes:** a named `t3-data` volume at `/data/t3`; a named `t3-gateway-auth`
   volume at `/data/gateway-auth` (rw in t3, ro in the gateway); and the gateway's
   existing workspaces volume at `/app/workspaces` in both.
4. **Gateway env** (the client lane defines the exact names): T3 URL `http://t3:3773`
   and the token as a `_FILE` secret pointing into the `t3-gateway-auth` mount.
5. **Bootstrap and rotation:** a mise or stack task that runs
   `docker compose exec t3 t3-issue-gateway-token` after first deploy, plus a reminder
   or timer to rotate before the 30-day expiry and revoke the previous session.
6. **Optional:** `T3_CODEX_CONFIG_EXTRA_FILE` with `[mcp_servers.*]` once the gateway
   exposes Linear host tools as an MCP server. The Claude side needs its own MCP
   config, which this sidecar doesn't render yet.

## Known limitations

- The WS RPC contract is internal to T3 and changes between releases. The pin keeps
  it stable; read the upgrade procedure before bumping.
- The gateway token isn't an `mcp-client` session, so it can't use `/mcp`. That's
  fine for the WS driver.
- WS `orchestration.launchThread` accepted `runtimeMode: "full-access"` from this
  token. The MCP path's runtime-mode ceiling doesn't apply to the WS path, so
  `orchestration:operate` is effectively full-access. Guard the token file.
- Codex fetches its plugin marketplace (`github.com/openai/plugins`) into
  `CODEX_HOME/.tmp` at session start. That's harmless but needs outbound GitHub
  access.
