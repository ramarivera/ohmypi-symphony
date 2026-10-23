# T3 sidecar deployment contract

Beads `sym-gmy.2`, `sym-gmy.4`, `sym-gmy.5`. The gateway driver uses T3's
WebSocket orchestration API. T3 starts Codex and Claude Code, whose model
requests go to CLIProxyAPI. This PR doesn't deploy the stack.

## Image

Build from the repository root:

```sh
docker build -f deploy/t3/Dockerfile -t ohmypi-symphony-t3:local .
```

| Component | Version | Verification |
| --- | --- | --- |
| T3 | 0.1.2 | Release SHA256SUMS, amd64 and arm64 |
| Codex | 0.156.1 | Main's shared `deploy/agent-clis/install-agent-clis.sh` checksums |
| Claude Code | 2.1.304 | Same shared installer and version as the gateway |

The shared installer is the only CLI download/checksum implementation. Codex
0.156.1 is the version main's installer supports and T3 0.1.2 pins for managed
installs. Update both architectures in that installer when changing CLI pins.
T3's own tarball checksums remain in its Dockerfile. Recheck the T3 provider
settings and WS protocol against the new release before upgrading T3.

The Debian trixie image includes git, SSH, ripgrep, jq, curl, libatomic1 and
tini. Claude's executable uses main's `claude-cliproxy` wrapper. Auto updates
are disabled. No direct provider credential is required or installed.

## Driver contract

| Setting | Value |
| --- | --- |
| Sidecar service and container | `t3`, `ohmypi-symphony-t3` |
| Gateway `T3_BASE_URL` | `http://t3:3773` |
| Gateway `T3_TOKEN_FILE` | `/run/secrets/t3-gateway-token` |
| Compose auth secret | `t3-gateway-token`, host `./secrets/t3-gateway-token` |
| Default `T3_RUNTIME_MODE` | `auto`, `full-access` requires explicit opt-in |
| Workspace path, both containers | `/app/workspaces` |
| Sidecar uid/gid | `2001:2001`, reserved outside OMP slots `1001..1064` |
| Provider instances | `codex-cliproxy`, `claude-cliproxy` |
| WebSocket protocol | `2` |

The gateway exchanges its bearer at `POST /api/auth/websocket-ticket`, then
connects to `/ws?wsTicket=...&orchestrationProtocol=2`. Issue the bearer with
`orchestration:read` and `orchestration:operate`. These scopes permit project
registration, `orchestration.launchThread`, dispatch, subscribe and projection.
The token isn't an `mcp-client` session, so `/mcp` rejects it. The healthcheck
uses `/.well-known/t3/environment`; it doesn't prove provider readiness.

## Workspace permissions and isolation

Main allocates a different worker uid for each session and uses mode-0700
workspace directories. A shared mount doesn't grant T3 access to them. T3 must
not reuse the first worker uid, run as root, gain DAC_OVERRIDE, or relax every
workspace to group/world access.

The sidecar has a dedicated uid/gid 2001, no Linux capabilities and
`no-new-privileges`. The driver must grant that identity access only to the
checkout prepared for a T3 session, and revoke access after stopping all work
on it. For example, the gateway can transfer ownership of a checkout that is
exclusively reserved for T3, and then reclaim it after the T3 thread exits.
Don't transfer an active OMP workspace or reuse its uid reservation. A source
change to the driver/allocator is still required; this image doesn't implement
it and deliberately leaves mode-0700 OMP directories inaccessible.

All threads inside one T3 daemon share uid 2001 and its T3 data. This doesn't
provide isolation between simultaneous T3 sessions. Don't enable mutually
untrusted T3 sessions until the driver has per-session sidecars/identities.
Toolbox defaults `ohmypi_symphony_t3_enabled` to false pending this integration
and the token bootstrap. This is an acceptance blocker, not a working
per-session isolation claim. Coordinate it with PR #16.

Mount only `gateway-workspaces:/app/workspaces`, `t3-data:/data/t3`, and
`t3-gateway-auth:/data/gateway-auth` in T3. Never mount gateway data, logs,
secrets, worker profiles, Nix roots or a host Docker socket. Existing sidecar
volumes from uid 1000/1001 require a stopped backup and an explicit ownership
migration to uid 2001. Don't recursively chown the shared workspace volume.

## Proxy configuration

Supply `CLIPROXYAPI_BASE_URL=https://cliproxyapi.ai.roxasroot.net` and exactly
one of `CLIPROXYAPI_API_KEY` or `CLIPROXYAPI_API_KEY_FILE`. The deployed stack
uses the latter, with a separate mode-0400, uid-2001 proxy file. The gateway
keeps its own root-owned copy. The proxy is currently keyless; the existing
fnox deploy credential is still passed as a bearer so future key enforcement
doesn't require changing this contract. No live proxy configuration changes
are part of this PR.

Every startup renders private mode-0600 configuration:

- `$T3CODE_HOME/codex-cliproxy/config.toml` sets the Responses provider to
  `<proxy>/v1`, reads the bearer from `CLIPROXYAPI_API_KEY`, and defaults to
  `gpt-6-sol` with `high` effort. `T3_CODEX_MODEL` and
  `T3_CODEX_REASONING_EFFORT` override those defaults. The CLI config sets
  approval policy `never` and sandbox `danger-full-access`; the driver still
  selects the T3 runtime mode explicitly.
- `$T3CODE_HOME/userdata/settings.json` enables only the two proxy instances,
  disables all persisted other instances, clears project provider overrides,
  and routes text generation to `codex-cliproxy`. The default thread model is
  `claude-opus-5-5`; select it with `T3_DEFAULT_PROVIDER_INSTANCE` and
  `T3_DEFAULT_MODEL`. Only the two proxy instance ids are accepted.
- Claude's sensitive bearer is in that private settings file. T3 redacts it
  in API responses. Its home is `$T3CODE_HOME/claude-cliproxy`.

The entrypoint drops inherited OpenAI/Anthropic credentials and Claude OAuth
or alternate cloud-provider selectors. Main's Claude wrapper enforces the
proxy again at CLI launch. The managed allowlist is reapplied on restart;
an authenticated T3 operator can change live settings, so this isn't a
network egress firewall. Trust the orchestration bearer accordingly.
`T3_CODEX_CONFIG_EXTRA_FILE` is a trusted operator TOML extension, not a
workspace-provided file. No host login directories are mounted.

## Linear MCP stub, sym-gmy.5

Main at `26efcab` has no gateway endpoint exporting `linear_*` host tools.
`src/http/router.ts` has the MCP OAuth callback but no MCP server route;
`src/services/worker-mcp-proxy.ts` proxies configured external MCP servers on
worker loopback and isn't an export of gateway Linear host tools.

Consequently no Claude MCP endpoint, guessed URL, or shared Linear token is
rendered. The future integration must use the gateway's authenticated MCP
endpoint and the single shared Linear token chosen in `sym-gmy.6`. Render a
Claude-supported MCP configuration when that server contract exists, and test
an actual Linear tool call through it. Codex's trusted extra-TOML hook remains
available. This documented stub satisfies the fallback for `sym-gmy.5`; it
doesn't establish Claude Linear host-tool acceptance.

## Stack wiring and token lifecycle

The Toolbox PR adds an isolated `t3-backend` bridge shared with the gateway,
without a host port or edge route. It permits outbound HTTPS to CLIProxyAPI.
T3 has a container memory limit because no systemd user bus is installed;
T3 falls back to ordinary child processes without per-thread cgroup limits.

The parent deploys via Toolbox's normal Chezmoi and `mise run stacks:up`
workflow. Pre-up writes a separate uid-2001 proxy credential and an empty
root-owned mode-0400 gateway token placeholder on first boot. It preserves
issued gateway tokens on rerun. It doesn't enable T3 automatically.

After starting the sidecar, run the managed Toolbox script:

```nu
nu ~/stacks/ohmypi-symphony/rotate-t3-token.nu
```

This runs `t3-issue-gateway-token` in the container, which stages a 30-day
bearer and non-secret session metadata in `/data/gateway-auth`. The Toolbox
script copies the bearer into a separate root-owned mode-0400 file for the
gateway. T3 can't replace that root-owned file through the mounted secret.
It doesn't restart services. After installing the token, explicitly enable
the registry setting only once workspace access is implemented, apply the
Compose target, and recreate the gateway to pick up the new secret inode:

```nu
mise run stacks:up ohmypi-symphony -d --force-recreate gateway
```

Rotate before the metadata's expiry, recreate the gateway and verify its
connection, then revoke the prior session. Save the old session id before
issuance; issuance metadata is replaced. There's no automatic renewal.
T3 prints a one-time pairing credential on startup. Keep logs local, or
filter pairing credential lines before forwarding them.

## Verification boundary

The refreshed image built on workbench. All download checksums passed, and
the CLIs report T3 0.1.2, Codex 0.156.1 and Claude Code 2.1.304 as uid 2001.
`mise exec -- bun run check` passed with 772 tests, including real entrypoint
render tests for proxy enforcement, private modes, inherited credential
removal and invalid input failures. Toolbox's focused tests cover Compose
exposure, shared paths, secret provisioning and token rotation.

No deployment to arch-cachy, Linear agent session, remote model prompt, or
end-to-end PR creation was performed by this lane. Earlier PROBE-OK receipts
under `~/dev/agentic-scratchpads/ohmypi-symphony/t3-sidecar/` used uid 1000 and
aren't runtime acceptance of this revision. Parent acceptance must exercise
both providers, gateway workspace access, token rotation and the real Linear
session-to-PR flow with the merged driver.
