# Gateway security and deployment

All registered HTTP routes require an admin session except `GET /`, `GET /health`,
`GET /oauth/start`, `GET /oauth/callback`, and HMAC-verified
`POST /webhooks/linear`. Run pages and JSON require the run's organization;
HTML login redirects preserve a local return path. Webhook bodies and MCP OAuth
responses have a 1 MiB streaming limit.

## Tenant admission

Set `LINEAR_ALLOWED_ORGANIZATION_IDS` to comma-separated Linear organization IDs.
Production startup fails if it's missing. Development with the setting absent
admits all organizations, so don't expose a development gateway publicly.
Changing the list takes effect on restart and also blocks existing admin sessions
for removed organizations. Installation revocation deletes that org's sessions.

Set `GITHUB_APP_ALLOWED_OWNERS_JSON` to an authoritative mapping such as
`{"linear-org-id":["github-owner"]}`. Owners are case-insensitive. A missing org or
owner binding denies token minting, even if the GitHub App is installed there.
Tokens remain scoped to the requested repository. Configure `GITHUB_APP_ID` and
`GITHUB_APP_PRIVATE_KEY` or its `_FILE` alternative as before.

Shared Nix cache list/prune operations require an organization in
`GATEWAY_ADMIN_ORGANIZATION_IDS`; ordinary tenant admins can't access them.
An absent list grants nobody gateway-wide cache administration.

Repository URLs must be canonical `https://github.com/owner/repo` URLs, optionally
ending in `.git`. Convert existing SSH/local/non-GitHub repository records before
running them. GitHub tokens use Git environment configuration during clone, and
mode-0600 local Git configuration for worker access, never command arguments.
The worker can read that repository-scoped installation token, which GitHub
expires after one hour. This is intentional for authenticated worker Git
operations. It is not the GitHub App private key or an organization-wide token.
Hiding a token from code that can invoke authenticated Git requires a separate
Git broker, which this design doesn't provide. Gateway header refresh and
terminal cleanup remove only generated values and retain user-installed headers.

## Docker worker identity

The Docker entrypoint runs the gateway as root and locks `/app/data` and
`/app/logs` to mode 0700. Workers get distinct numeric uid/gid pairs from
`WORKER_UID_BASE` through `WORKER_UID_BASE + WORKER_UID_COUNT - 1` via `setpriv`, with no
supplementary groups, capabilities, or privilege escalation. Their HOME and
TMPDIR point to excluded `.omp-home/` and `.omp-tmp/` directories. The gateway
assigns each workspace root mode 0700 and its files to the session uid without following symlinks and rejects
multiply-linked regular files that require an ownership change. Already-owned
hardlinks remain valid across retries and resumes. MCP materialization, cleanup,
and Git credential updates execute as the worker uid, including on resume, so
worker-planted paths never cause privileged gateway writes. Git subprocesses receive a transport
environment allowlist, without gateway or model-provider credentials.
The workspace helper starts from its gateway-owned installation directory and
receives the workspace as an absolute argument. Both asynchronous operations and
synchronous retention pin Bun's configuration to `/dev/null`, disable `.env`
autoloading and automatic package installation, and use an environment allowlist
that excludes `BUN_*`, `NODE_OPTIONS` and loader injection variables. Keep the
helper, its installation directory, dependencies and parent directories unwritable
by the worker uid. Repository `bunfig.toml`, `.env`, `package.json`, `tsconfig.json`
and `node_modules` are never helper startup inputs. Cleanup uses explicit workspace
paths and doesn't depend on the helper's cwd.

MCP configuration is projected to its generated `mcpServers` fields before helper
stdin is serialized. OAuth client records and renewal fields never cross that uid
boundary. Agent database inputs are empty for gateway-managed OAuth servers.
Generated MCP configuration contains only a loopback URL and the worker session's
proxy bearer; upstream access tokens stay in the gateway. Git persistence sends
only the repository-scoped header string on stdin, never argv; cleanup operations
carry no credential payload.

Retention removes workspace contents as the worker uid while holding its synchronous
SQLite reopen fence. Before delegation, the gateway migrates legacy ownership with
the same descriptor-based, no-follow walk used for workspace preparation. Only
canonical paths strictly inside the workspace root qualify. The gateway only
removes the empty top-level directory; it never recursively deletes a
worker-controlled tree with gateway privileges. A
failed cleanup leaves the workspace records intact for a later sweep.
`/app/workspaces` must be a separate writable volume,
outside `/app/data`; the image sets `WORKSPACE_ROOT=/app/workspaces`.

Remove deployment overrides that force the gateway to run as `bun`. Keep the
entrypoint capability bounding policy. It drops all capabilities except CHOWN,
DAC_OVERRIDE, KILL, SETUID, SETGID and SETPCAP. KILL lets the gateway terminate
workers running under another uid. DAC_OVERRIDE is needed to traverse
worker-owned mode-0700 directories during the ownership walk, and SETPCAP is
needed for the worker's empty capability bounding set. Each worker has no
capabilities. A deployment may also apply `--cap-drop=ALL` with these six
`--cap-add` values; omitting DAC_OVERRIDE or SETPCAP breaks workspace preparation
or worker startup.
Mount all gateway secret files root-owned with mode **0400**, including Docker
secrets. Startup rejects insecure `_FILE` mounts. Existing data/log volume roots
must be real directories and are secured by the entrypoint before the gateway
starts. Before dropping capabilities, the entrypoint migrates legacy ownership
of regular files and directories under `/app/data` to root:root, including nested
database paths and SQLite `-wal`, `-shm` and `-journal` sidecars. It skips symlinks.
Foreign-owned multiply-linked files fail startup; replace them with independent
copies before upgrading.
Keep all gateway state that needs ownership migration inside `/app/data`, and
stop the old gateway before starting the upgrade. Custom `DATABASE_PATH` values
outside that directory require an operator-managed ownership migration.

Workspace ownership is migrated separately by preparation and retention under
`/app/workspaces`. Existing log files and Nix roots don't receive a recursive
ownership migration: gateway log append/rotation and Nix root creation/removal
don't chmod legacy files and retain access through DAC_OVERRIDE. Nix roots contain
result symlinks into `/nix`; their targets must never be recursively chowned by
this migration. The `/nix` store retains its Nix-managed ownership. The gateway
and worker Nix configuration is described below.

Gateway auth/key mounts and Docker secrets outside `/app/data` aren't modified;
provision them root-owned with mode 0400 before upgrading, including readonly mounts.
Don't share gateway state or key volumes with workers or mount the Docker socket
into the container.

Docker defaults to a dedicated range of 64 identities starting at 1001. Override
it with `WORKER_UID_BASE` and `WORKER_UID_COUNT`. `WORKER_UID` alone is deprecated
and migrates to a range of size one. An explicit base wins over the legacy value;
a base without a count defaults to 64. Don't overlap gateway/system accounts or
other processes with this range. Root is forbidden. No passwd/group entries or
new capabilities are required for the numeric identities.

The `worker_identity` SQLite table associates each session with its uid and has a
unique index on active uids. Allocation uses an immediate transaction, so separate
gateway connections can't allocate the same active uid. Recovery and resume reuse
the persisted uid. A terminal session releases its slot after worker teardown.
Before release, the gateway kills remaining processes with that uid, including
escaped process groups, and seals all its retained workspace and clone roots to
root-owned mode 0700. It also removes its entries in `/tmp`, `/var/tmp`, and
`/dev/shm` through the installed helper running as the retired uid, with no capabilities. Cleanup never
follows symlinks or deletes shared entries with gateway privileges. That happens
before another session can acquire the slot. SIGKILL scans wait up to 500 ms for
kernel teardown. A failed retirement keeps the uid reserved and records a
`worker_uid_quarantined` run event. Both new allocations and retention retry
terminal allocations after their authority lease expires. One failed identity
doesn't abort cleanup of others.
A resumed terminal session requires its original uid; if that slot is occupied,
startup fails with an explicit busy error and follows the normal retry policy.
Exhaustion likewise fails clearly instead of sharing an identity. Size the range
for expected concurrency and resume demand. Retention reclaims terminal allocations
left by a crash once their authority lease has expired, including failed clones.

Stop the previous gateway container and all its workers before upgrading or
changing the range. Don't run old and new binaries against the same workspaces.
The entrypoint seals existing workspace roots before the gateway allocates any
uid, so legacy unresumed workspaces can't be exposed to the first new worker.
Legacy sessions get a mapping when next started; their no-follow ownership walk
migrates files to the allocated uid. Retention looks up the persisted uid and
migrates legacy workspaces without a mapping to the range base. It skips a
workspace if another active session holds that uid, rather than briefly exposing
old files during deletion. Failed cleanup keeps records for a later sweep.
Retained roots and abandoned clone roots stay inaccessible after slot reuse.

Each session has its own `.omp-home`, `.omp-tmp`, XDG config/data/cache directories,
and Bun install/transpiler caches. omp's writable agent configuration and MCP
SQLite database are also inside that session's private workspace. Existing
`/home/bun/.omp` configuration is not a shared worker HOME. The environment
allowlist forwards no direct model-provider keys or endpoints, including
`ANTHROPIC_*`, `OPENAI_*`, `GOOGLE_API_KEY`, `GEMINI_API_KEY` and
`OPENROUTER_API_KEY`. The one model credential
a worker sees is the dedicated CLIProxyAPI client key, which the gateway reads
from `CLIPROXYAPI_API_KEY` or `CLIPROXYAPI_API_KEY_FILE` and injects per run with
`CLIPROXYAPI_BASE_URL`. The worker can read it, so give Symphony its own
client key. CLIProxyAPI currently stays keyless, so the gateway still sends its
key but revocation won't take effect until CLIProxyAPI enforces client keys.
Gateway-only keys and arbitrary
environment variables don't reach workers. Skills, rules, extensions and
`models.yml` come from a validated, credential-free profile copied into the
private agent directory. omp gets no Anthropic credential or endpoint, so its
built-in Anthropic provider stays unavailable; Claude goes through
pi-claude-cli and Claude Code to CLIProxyAPI. See
[worker-profile.md](worker-profile.md).

Shared `/app/node_modules`, `/app/dist`, and Nix GC roots belong to root
and have no group/other write permissions. The Nix store keeps its Nix-managed
ownership. `/opt/nix` points into the shared profile without traversing a worker
HOME. Workers execute gateway-prepared binaries through PATH as described below;
no read-only store connection or store database access is introduced. Startup
repairs legacy shared executable ownership without recursively chowning `/nix`.
The workspace parent belongs to root with mode 0755 so a session can't rename
another session's root. No shared writable cache or supplementary group is granted.

Native launches without either uid setting retain same-uid workers. The
allowlist and mode-0600 SQLite files still apply, but same-uid workers can read the
gateway's process environment and files. Use Docker for this boundary. Per-session
uids isolate discretionary filesystem access; workers still share the container's
network, kernel and global process visibility. This isn't a network sandbox.

## Nix in Docker

The gateway has `HOME=/root`. Nix 2.30.3 is installed with `--no-daemon`;
`/etc/nix/nix.conf` enables `nix-command flakes`, disables
the Nix sandbox, and sets `build-users-group =` to empty for this single-user installation.
The empty group makes builds run as the Nix caller. The gateway's
`NixEnvironment` uses `nativeRunner` without a uid transition, so environment
preparation runs as root. Only use a trusted, pinned `NIXPKGS_FLAKE_REF`.
Don't expose a root Nix daemon to workers or run repository-supplied builders
as the gateway.

`/opt/nix/bin` is on PATH for both identities. Its image-owned profile link
points directly into `/nix/store`, outside bun's private home and outside the
persisted volume's profile directory. This also works with existing Nix
volumes. The gateway builds the configured repository packages, records their
closure metadata, and keeps GC roots for active runs. Session authority prepends
the realized store paths' `bin` directories to PATH before RPC worker launch.
Workers retain their workspace HOME and use those executables through ordinary
filesystem reads, including while the gateway prepares another repository.
`NIX_REMOTE`, `NIX_CONFIG`, `NIX_PATH`, and `NIX_PROFILES` aren't forwarded at
either worker environment boundary. Workers don't need store database access.

Worker-side `nix eval`, `nix build`, and other store commands are unsupported.
In the isolated Docker worker, default store access fails on the shared store's
permissions rather than selecting an immutable database connection. Request
packages through the repository's `nixPackages` configuration so the gateway
prepares them before launch. This environment contract isn't a sandbox against
arbitrary worker code; native same-uid workers still have their user's access.

Don't use `local?read-only=true` against the live shared store. In
[Nix 2.30.3's local-store configuration](https://github.com/NixOS/nix/blob/2.30.3/src/libstore/include/nix/store/local-store.hh),
read-only mode disables locking and requires an immutable database. The
[local-store implementation](https://github.com/NixOS/nix/blob/2.30.3/src/libstore/local-store.cc)
selects `SQLiteOpenMode::Immutable`, and
[the SQLite wrapper](https://github.com/NixOS/nix/blob/2.30.3/src/libstore/sqlite.cc)
opens it with `immutable=1`.
[SQLite skips locking and change detection in that mode](https://sqlite.org/uri.html#uriimmutable),
which can return incorrect results or corruption errors under concurrent writes.
No store ownership migration or extra capability is required. Never recursively chown `/nix/store`
or follow GC-root symlinks to change ownership.

## MCP OAuth

Discovery and token requests resolve DNS, reject non-public addresses, and pin the
connection to a vetted address. Redirects use manual handling and repeat DNS
validation. Only same-origin GET redirects are followed; POST and cross-origin
redirects fail. Responses are bounded while streaming and have a five-second
request timeout. `MCP_OAUTH_ALLOW_LOOPBACK_HTTP=true` explicitly enables local
HTTP development. It doesn't allow private LAN or metadata endpoints.

Gateway-managed OAuth servers use a per-worker MCP proxy listening on IPv4
loopback at an ephemeral port. Each worker gets a random 256-bit bearer scoped to
its configured OAuth servers. No upstream access token, refresh token, OAuth
client secret or token endpoint crosses the worker boundary. The agent database
is still created through the worker-uid helper, with no OAuth rows. A tracked or
foreign `mcp.json` causes startup to fail when a proxy is required, so a skipped
write can't silently bypass renewal.

The gateway checks credentials before every proxied request, refreshing within
60 seconds of expiry and retrying one upstream 401 with a renewed token. Refreshes
single-flight per organization/server credential, including simultaneous requests
from different sessions. A second credential read after claiming the flight
prevents replay of a rotated refresh token from a stale reader. Refresh failures
return a generic 502 and persist an `mcp_refresh_failed` run event naming the
server and asking the operator to reconnect it. Provider response bodies and
credential values never enter those events. A subsequent request can retry after
repair; a missing or changed credential fails closed.

The proxy forwards MCP HTTP requests and streams, session/protocol headers, and
legacy SSE endpoint events. Legacy endpoints become opaque local routes and must
retain the configured upstream origin. Workers can't supply arbitrary targets or
upstream authorization. Upstream DNS is validated and pinned, redirects are
rejected, request bodies and legacy SSE events are bounded, and only protocol
response headers reach workers. Closing or canceling a worker, losing its lease,
failed startup, or gateway shutdown stops its listener and aborts its streams.
The main HTTP listener has no new public route. The proxy bearer lives only in
mode-0600 `mcp.json` under the session's mode-0700 workspace. Peer session uids can't read that config or the private
agent database; knowing another loopback proxy URL doesn't authorize requests.
Native same-uid filesystem limitations still apply.

The runtime is pinned to OMP 18.8.6 in `package.json` and `Dockerfile`. Its
`pi-ai/src/auth-storage.ts` caches credentials in `#data`, while
`pi-coding-agent/src/mcp/manager.ts` resolves OAuth on connection and auth failure
and retains resolved transport headers. Auth resolution can re-read SQLite rows,
but it doesn't poll the file per request; the existing materializer replaces the
database file instead of updating the open database. A file-based approach would
need safe in-place updates and auth-failure/reconnect coordination.
`config/resolve-config-value.ts` caches successful `!command` output for the
process lifetime. The proxy keeps OMP's bearer stable while rotating upstream
credentials. OMP's MCP refresh callback requires local refresh material and
explicitly rejects its remote-broker refresh sentinel, so its model auth broker
isn't a drop-in MCP renewal path here. This per-session bearer pattern overlaps with `sym-gmy.6`; hosting Linear
tools for T3 remains separate work.

## Encryption rotation

Cipher version 2 carries a key id and authenticates table, organization, row id
and field with AES-GCM AAD. The key comes from `GatewayConfig`, including
`TOKEN_ENCRYPTION_KEY_FILE`. The default key id is 1.

Before rotating, stop all gateway instances and back up the database. Set the new
`TOKEN_ENCRYPTION_KEY` and a new `TOKEN_ENCRYPTION_KEY_ID` between 1 and 255, and put
retained old keys in `TOKEN_ENCRYPTION_PREVIOUS_KEYS_JSON`, e.g.
`{"1":"<old-base64-key>"}`. The JSON setting also supports `_FILE`; mount that file
as a mode-0400 secret in Docker. Never reuse an id for a different retained key.

Startup atomically upgrades encrypted version-1 rows and rows using retained keys
to the current key and row-specific AAD. Version-1 decryption remains supported
for compatibility. Keep old keys until every database/backup that needs them has
been upgraded. Old binaries can't read version-2 rows; rolling back requires the
pre-upgrade database backup. No Effect version change is included.

Security tests use canonical fixtures under
`~/dev/agentic-scratchpads/ohmypi-symphony/security/tests` to honor the global
scratch-storage policy and avoid a quota-limited shared `/tmp`. CI can override
the fixture parent with `SECURITY_TEST_TMPDIR`; per-case directories are removed
after each test.

## Two-session Docker isolation probe

Build the candidate image, then run a disposable container with the entrypoint and
only the existing capability set. Use a dedicated test range and fresh workspaces:

```sh
docker build -t symphony-uid-isolation:probe .
docker run --rm --cap-drop=ALL --cap-add=CHOWN --cap-add=DAC_OVERRIDE \
  --cap-add=KILL --cap-add=SETUID --cap-add=SETGID --cap-add=SETPCAP \
  -e WORKER_UID_BASE=21001 -e WORKER_UID_COUNT=2 \
  symphony-uid-isolation:probe bun /path/to/mounted/probe.ts
```

The mounted probe must use `SqliteClientLive`, `makeWorkerIdentityRepo`, workspace
preparation/helpers and `RpcWorker.spawn` with the allocated `workerUid`. Start
two real `omp --mode rpc` workers concurrently. As each identity, assert own
workspace writes succeed and the other workspace, `.git/config` and
`.omp-gateway/agent.db` reads/writes fail. Verify gateway database, secret and
`/proc/1/environ` reads fail; shared Nix/profile/tool writes fail; CapEff and CapBnd
are zero and NoNewPrivs is one. Verify 0700 root ownership, persisted recovery,
exhaustion, release/recycle with the old workspace still denied, busy resume,
same-uid resume after release, escaped-process termination, retention skipping
busy identities and legacy cleanup. Create a uid-1001 legacy workspace before
entrypoint startup and verify it is sealed before allocation.

Save the script, image digest, build log and JSON receipt under
`~/dev/agentic-scratchpads/ohmypi-symphony/uid-isolation/`. Use dummy credentials
only. Unit tests cover allocation and filesystem syscall contracts; the real
Docker probe establishes the uid access boundary without running a model turn.
