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
are secured by the entrypoint before the gateway starts. Don't share gateway
state or key volumes with workers or mount the Docker socket into the container.

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
