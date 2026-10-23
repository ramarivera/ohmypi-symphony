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
boundary. Agent database inputs contain only server URLs and short-lived access
tokens with expirations. Git persistence sends only the repository-scoped header
string on stdin, never argv; cleanup operations carry no credential payload.

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

Native launches without `WORKER_UID` retain same-uid workers. The environment
allowlist and mode-0600 SQLite files still apply, but same-uid workers can read the
gateway's process environment and files. Use the Docker deployment for the uid
boundary. Workers currently share one uid and can access other workers'
workspaces; this change doesn't provide per-run or per-tenant filesystem/network
isolation. Model-provider API keys explicitly allowed for omp remain visible to
the worker; gateway-only keys and arbitrary environment variables don't.

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

Workers receive only access tokens expiring within one hour. Refresh tokens,
OAuth client secrets and token endpoints stay in the gateway. Providers issuing
non-expiring or longer-lived access tokens fail worker startup; configure a
shorter token lifetime at the provider. A running worker can't renew its own MCP
token. Long-running sessions may need a gateway-managed restart/reconnect after
expiry; this change doesn't add live token delivery to an already-running omp.

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
