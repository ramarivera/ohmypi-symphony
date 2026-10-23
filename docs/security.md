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
`/app/logs` to mode 0700. Workers run as uid/gid 1001 through `setpriv`, with no
supplementary groups, capabilities, or privilege escalation. Their HOME and
TMPDIR point to excluded `.omp-home/` and `.omp-tmp/` directories. The gateway
assigns workspace files to that uid without following symlinks and rejects
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
this migration. The `/nix` store retains its Nix-managed ownership. The root
gateway also needs a root-readable single-user Nix configuration: the legacy
uid-1000 HOME at `/home/bun` causes Nix to fall back to `/root` and miss its
experimental-feature settings. This separate deployment issue is tracked in
`sym-2sm.18`.

Gateway auth/key mounts and Docker secrets outside `/app/data` aren't modified;
provision them root-owned with mode 0400 before upgrading, including readonly mounts.
Don't share gateway state or key volumes with workers or mount the Docker socket
into the container.

Native launches without `WORKER_UID` retain same-uid workers. The environment
allowlist and mode-0600 SQLite files still apply, but same-uid workers can read the
gateway's process environment and files. Use the Docker deployment for the uid
boundary. Workers currently share one uid and can access other workers'
workspaces; this change doesn't provide per-run or per-tenant filesystem/network
isolation. Model-provider API keys explicitly allowed for omp remain visible to
the worker; gateway-only keys and arbitrary environment variables don't.

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
The main HTTP listener has no new public route. Existing same-uid and peer-worker
filesystem limitations still apply to session bearers.

The runtime is pinned to OMP 17.1.8 in `package.json` and `Dockerfile`. Its
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
