# AGENTS.md

Canonical instructions for coding agents in `ramarivera/partitura`
(package `partitura`). `CLAUDE.md` imports this file.

Partitura is a Bun + Effect v4 service that receives Linear agent-session
webhooks, runs Oh My Pi (`omp`) workers over RPC in per-session workspaces, and
projects worker activity back to Linear. State is one SQLite file.

Reference docs:

- `docs/security.md`: deployment and security model (tenant admission, worker
  uid isolation, Nix, MCP OAuth, key rotation, Docker probe). Read it before
  touching auth, workers, workspaces, Docker, or Nix.
- `LINEAR_GATEWAY_RESEARCH.md`: original design research. Historical; the code
  wins where they disagree.

## Commands

Tool versions are pinned in `mise.toml` (Bun 1.3.14, fnox). Run Bun through mise.

```bash
mise run install                    # bun install --frozen-lockfile; required in a fresh worktree
mise exec -- bun run check          # typecheck + lint + test + build; the merge gate (~15s)
mise exec -- bun run typecheck      # tsc --noEmit, then effect:check
mise exec -- bun run effect:check   # effect-tsgo diagnostics, errors and warnings fail
mise exec -- bun run lint           # biome check .
mise exec -- bun run format         # biome format --write .
mise exec -- bun run test           # effect:check, then bunx --bun vitest run
mise exec -- bun run build          # bundles index, report-deviation extension, workspace-worker into dist/
mise exec -- bunx --bun vitest run test/store.test.ts   # one file
mise exec -- bunx --bun vitest run -t "test name"       # by name
mise run dev                        # fnox exec -- bun --watch src/index.ts (needs fnox secrets)
mise run start                      # fnox exec -- bun src/index.ts
```

Without `node_modules`, `typecheck` fails with `Cannot find type definition file
for 'bun-types'`. Run `mise run install` first.


Vitest must run on the Bun runtime (`bunx --bun`): source and tests import
`bun:sqlite` and `Bun.*`. Plain `npx vitest` does not work.

Configuration comes from environment variables; `.env.example` lists them with
comments. Secrets accept a `<NAME>_FILE` alternative (Docker secrets).

## Layout

```
src/index.ts                 BunRuntime.runMain(Effect.scoped(main))
src/conductor.ts             Layer graph, recoverGateway, schedulers, HTTP server
src/http/router.ts           Routes, authenticateRoutes, publicRoute, /health
src/domain/                  errors.ts (Schema.TaggedError), ids.ts (branded ids), models.ts (wire schemas)
src/services/                One Context.Service per file (see map below)
src/services/store/          SQLite client, migrations, repositories
src/admin-ui/                Server-rendered admin HTML/CSS
src/extensions/report-deviation.ts   omp extension: rromp_report_deviation tool
src/workspace-worker.ts      Helper process run as the worker uid for writes into workspaces
test/*.test.ts               Vitest suites, one per area
test/fixtures/               fake-rpc, per-test temp paths, v3 wire-compat fixtures, crash harness
```

## Architecture

### Startup (`src/conductor.ts`)

`GatewayServicesLive` merges every service layer over `SqliteClientLive` and
`GatewayConfig`. Order on boot:

1. Open SQLite, apply pragmas (WAL, foreign keys, busy timeout), run `migrate`.
2. `recoverGateway`: `RunRepo.recoverInterruptedRuns()` then
   `WebhookPipeline.recoverPendingDeliveries()`. This must finish before the
   HTTP server accepts deliveries or the reconciler starts. A storage failure
   during recovery fails startup.
3. HTTP server (`BunHttpServer` + `RouterLive`).
4. Scheduled reconciler (periodic `tick` plus webhook-triggered `tick`) and
   `Retention.run`, both `Effect.forkScoped`.

Keep this ordering when adding layers. Anything that consumes deliveries or
runs must be provided after `recoveredServices`.

### Request path

- `POST /webhooks/linear` -> `WebhookPipeline.handle`: 1 MiB bounded body,
  HMAC-SHA256 signature check against `LINEAR_WEBHOOK_SECRET`, timestamp window
  (`WEBHOOK_REPLAY_WINDOW_MS`), org allowlist, then a durable delivery claim.
  On success the router triggers a reconciler tick.
- `DeliveryRepo` stores each delivery with `payload_hash` (hash of the raw HTTP
  bytes, sender dedupe) and a replay envelope: `payload_version` (1),
  normalized `payload_json`, and `replay_hash`, an HMAC keyed by HKDF of the
  webhook secret over `[version, organizationId, ingressHash, payloadJson]`
  (`deliveryReplayMac`). `claim_token` fences every write: `processClaim`,
  `mark`, and `releaseClaim` only act for the current token holder.
- Recovery replays `pending` rows only if `payload_version === 1` and the replay
  MAC verifies. Legacy rows (version 0) and tampered rows are marked failed,
  never processed. Replay re-runs tenant and identity validation through the
  same `processAuthenticatedEvent` path as live ingress.
- `Reconciler` drains runnable runs into `SessionAuthority`, plus a bounded
  catch-up poll of Linear (25 candidates per sweep).
- `SessionAuthority` owns the run lifecycle: lease fencing, workspace prep,
  Nix env, worker uid allocation, `RpcWorker` start, host tools (Linear
  mutations serialized with `cancel` through a per-session gate), stop/resume.
- `RpcWorker` spawns `omp` in RPC mode and streams events. `ActivityProjector`
  turns RPC events into Linear agent activities with retry/backoff.
  `RunEventRepo` stores events for the admin UI.

### Services (`src/services/`)

| Service | Role |
| --- | --- |
| `GatewayConfig` (`config.ts`) | Env-driven config, `_FILE` secrets, `organizationAllowed` |
| `WebhookPipeline` (`webhook.ts`) | Ingress, validation, durable claim, replay |
| `Reconciler` | Tick loop, trigger queue, Linear catch-up |
| `SessionAuthority` | Run lifecycle and worker orchestration |
| `RpcWorker` | `omp` subprocess over RPC, host-tool bridge |
| `ActivityProjector` (`projector.ts`) | RPC events -> Linear activities |
| `LinearGateway` | Linear SDK client with per-org queueing |
| `Workspace` | Clone/prepare/cleanup per-session workspaces |
| `NixEnvironment` | Pinned nixpkgs package envs, cache, GC roots, store budget |
| `GitHubApp` | Repo-scoped installation tokens, owner allowlist |
| `OAuth` / `McpOAuth` | Linear install OAuth; MCP server OAuth and refresh |
| `Admin` | Admin HTML and `/api/admin/*` handlers, sessions, CSRF |
| `Retention` | Hourly sweep of terminal workspaces, events, payloads, auth rows; Nix budget |
| `Executor` | Executor instance attach/toolkits for workers |
| `TokenCrypto` | Encryption of stored tokens with key rotation |

### Persistence (`src/services/store/`)

- Repositories are `Context.Service`s that run raw SQL against `bun:sqlite`
  through `tryDb(() => ..., "Repo.op")` and decode rows with
  `decodeRow`/`decodeRows` (Schema). Write counts go through `runChanges`.
- `transact(db, effect)` wraps `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK`, is
  interrupt-safe, and nests: inner `transact` calls join the outer transaction.
  All DB access shares one semaphore outside a transaction.
- Migrations live in `migrate()` in `sqlite-client.ts` and are additive:
  `CREATE TABLE IF NOT EXISTS`, and `ALTER TABLE ... ADD COLUMN` guarded by
  `PRAGMA table_info`. New columns need a default or must be nullable so old
  rows stay valid. Don't drop or rename columns in place; follow the
  `activity_projection_v2` copy-and-swap pattern if a rebuild is unavoidable.
- `Retention` runs its sweep synchronously in a `db.transaction(...).immediate()`
  because workspace removal holds a reopen fence that must not yield.
- Run-event pagination: `RunEventRepo.list(sessionId, { limit, cursor })` with
  `{ createdAt, sourceKey }` cursors; the admin run JSON route reads `limit`
  (default 100) and `cursor` query params.

### Admin and auth (`src/http/router.ts`, `src/services/admin.ts`)

- Public routes: `GET /`, `GET /health`, `GET /oauth/start`,
  `GET /oauth/callback`, `POST /webhooks/linear`. Everything else requires an
  admin session cookie (`omp_gateway_admin`) whose org passes
  `organizationAllowed`.
- Unauthenticated HTML GETs redirect to `/oauth/start?returnTo=...`
  (`safeReturnTo` allows local paths only); API/JSON requests get 401.
- Mutating admin API calls go through `requireMutation`: session, same-origin
  check, and an `x-csrf-token` derived from the session token.
- Run pages and JSON are scoped to the session's organization.
- `/health` returns 503 when the reconciler is stalled.

### Worker security model

Summary only. `docs/security.md` is authoritative.

- In Docker the gateway runs as root with a reduced capability bounding set
  (`docker-entrypoint.sh` ends in `setpriv --bounding-set=-all,+chown,+dac_override,+kill,+setgid,+setuid,+setpcap`).
- Each session gets a distinct numeric uid/gid from `WORKER_UID_BASE` ..
  `WORKER_UID_BASE + WORKER_UID_COUNT - 1` (Docker default 1001..1064), entered
  through `setpriv` with no supplementary groups, capabilities, or privilege
  escalation. Unset in native dev means same-uid workers.
- Workspace roots are mode 0700 and owned by the session uid.
  `prepareWorkerWorkspace` (async, clone/resume) and
  `prepareWorkerWorkspaceSync` (retention) chown with a no-follow descriptor
  walk and reject foreign hardlinks.
- Writes into worker-controlled trees (MCP config, Git credential headers,
  cleanup) run as the worker uid through `src/workspace-worker.ts`, started
  from the gateway install dir with Bun config pinned to `/dev/null` and an env
  allowlist. The gateway only removes the empty top-level directory. This
  applies when a worker identity is configured; in native same-uid mode
  (no `WORKER_UID_BASE`) the gateway removes the workspace recursively itself.
- Worker and Git env come from allowlists (`workerEnvironment`,
  `gitEnvironment`). Gateway-only secrets and arbitrary variables (including
  `BUN_*`/`NODE_OPTIONS`) never reach workers. Direct provider credentials and
  endpoints (`ANTHROPIC_*`, `OPENAI_*`, `GOOGLE_API_KEY`, `GEMINI_API_KEY`,
  `OPENROUTER_API_KEY`) aren't forwarded. The only model credential workers
  receive is the dedicated CLIProxyAPI client key, injected per run with
  `CLIPROXYAPI_BASE_URL`. Codex/OpenAI models use the curated profile's
  CLIProxyAPI provider; Claude uses pi-claude-cli and `claude-cliproxy` to
  route Claude Code through CLIProxyAPI. See `docs/worker-profile.md`.
- T3 (`T3_MODE=launch`): the gateway starts one `t3 serve` per session under
  that session's uid, state in `<workspace>/.omp-home/t3`, loopback port from
  `T3_PORT_BASE`, bearer issued per launch. The bearer is sent only after
  `/proc/net/tcp{,6}` shows the session uid owns the port. A shared T3 daemon
  (`T3_MODE=remote`) is refused under uid isolation. See
  `docs/agent-drivers.md`.
- MCP: `startWorkerMcpProxy` binds one `127.0.0.1` listener per worker with a
  random bearer. Generated MCP config holds only the loopback URL and that
  bearer; upstream tokens and OAuth client records stay in the gateway. Only a
  fixed header allowlist crosses the proxy.
- Nix config is root-owned in `/etc/nix/nix.conf`; shared inputs
  (`/home/bun`, `/app/node_modules`, `/app/dist`, `/app/nix-roots`) are
  root-owned and not group/other writable.
- The entrypoint rejects `*_FILE` secrets that aren't root-owned mode 0400 and
  locks `/app/data`, `/app/logs` to 0700.

## Effect v4 idioms in this repo

Effect is pinned at `4.0.6` (`effect`, `@effect/platform-bun`, `@effect/vitest`).
v3 APIs such as `Context.Tag`, `Effect.Service`, `Effect.catchAll`, and
`Effect.fork` don't exist in 4.0.6; don't copy v3 examples.

- Services: `class X extends Context.Service<X>()("X", { make: Effect.gen(...) }) {}`
  with `static readonly layerWithoutDependencies = Layer.effect(this, this.make)`
  and `static readonly layer = this.layerWithoutDependencies.pipe(Layer.provide(...))`.
  Call sites use `yield* X` or `X.use((s) => s.method())`.
- Methods: `Effect.fn("Service.method")(function* (...): Effect.fn.Return<A, E> { ... })`.
- Errors: `Schema.TaggedError` classes in `src/domain/errors.ts`, tagged
  `@Gateway/<Name>`. Catch with `Effect.catchTag("@Gateway/DatabaseError", ...)`
  or `Effect.catch`.
- Schema v4: `Schema.Literals([...])`, `Schema.decodeUnknownEffect`,
  `Schema.NumberFromString.check(...)`, `Schema.optionalKey`. Wire schemas in
  `src/domain/models.ts` must stay byte-compatible with v3;
  `test/effect-v4-boundaries.test.ts` enforces it against captured fixtures.
- Fibers: `Effect.forkScoped` for service-lifetime loops,
  `Effect.forkChild({ startImmediately: true })` for child work,
  `Effect.forkDetach` only for in-flight work that must outlive the caller
  (Nix flight leader).
- `Deferred.done` resumes waiters inline in v4. Clear shared in-flight state
  (maps, flags) before completing the Deferred, or a retrying waiter can rejoin
  the finished flight. See `nix-environment.ts` and `github-app.ts`.
- `Context.Reference` carries ambient transaction ownership in `sqlite-client.ts`.

## Testing conventions

- Import `describe`, `expect`, `it` from `@effect/vitest`. Use `it.effect` for
  tests on the TestClock, `it.live` for real time, subprocesses, sockets, or
  SQLite interruption. Plain `vitest` imports are for lifecycle hooks
  (`beforeEach`/`afterEach`) and non-Effect helpers.
- Build layers per test: `SqliteClientLive(":memory:")` plus
  `ConfigProvider.layer(ConfigProvider.fromUnknown({...}))` instead of env vars.
- Tests are hermetic: temp dirs via `test/fixtures/paths.ts` (`testPaths()`),
  fake `omp` via `test/fixtures/fake-rpc.ts`. No network, no real Linear.
- Add regression tests in the matching `test/<area>.test.ts`. Don't weaken
  assertions to get green.

## Invariants (don't break)

- Recovery (`recoverGateway`) completes before ingress and reconciliation.
- Every webhook is signature-verified before it's stored; replay only runs on
  version-1 rows whose replay MAC verifies; all delivery writes are fenced by
  `claim_token`.
- All routes except the five public ones require an admin session for an
  allowed org; mutating admin API calls require CSRF.
- Workers never receive gateway-only secrets, direct model-provider keys,
  upstream MCP tokens, OAuth client records, or GitHub App private keys. The
  CLIProxyAPI client key and base URL are their only model credentials. Git
  tokens go via env/0600 config, never argv.
- The gateway never follows symlinks or recursively deletes inside a
  worker-owned tree with gateway privileges.
- Migrations stay additive; existing rows must decode after upgrade.
- Logs never include tokens or secrets. Webhook replay logs never include
  stored payloads, SQL errors, or schema causes.
- Webhook bodies and MCP/MCP OAuth responses keep their 1 MiB streaming limit.

## Beads

Work is tracked with `bd` (beads 1.3.x, issue prefix `sym-`). The Dolt database
lives in the main checkout's `.beads/` (gitignored); `bd` resolves it from any
worktree. The Claude SessionStart/PreCompact hooks in `.claude/settings.json`
run `bd prime`, which is the source of truth for the full workflow.

```bash
bd ready                                    # unblocked work
bd show sym-2sm.5                           # details, parent, deps
bd create --title="..." --description="..." --type=task --priority=2 [--parent=<id>]
bd update <id> --claim                      # claim before starting
bd close <id> --reason="..."                # when merged or done
bd dolt push                                # sync beads data to the git remote (refs/dolt/data)
```

- Use `bd` for task tracking. No TODO/progress markdown files in the repo.
- Commit, push, and `bd dolt push` only when the user or orchestrator
  authorizes it.

## Git and PRs

- Branch off `main`; one PR per change. Merges into `main` are squash merges
  with the PR number in the subject.
- Conventional commits: `feat(scope):`, `fix(scope):`, `refactor:`, `docs:`,
  `test:`. Scopes in use: `security`, `webhooks`, `admin`, `deploy`, `hygiene`.
- `mise exec -- bun run check` must exit 0 before pushing.
- Bot review comments gate merges: address or explicitly resolve them before
  merging.
- Never bypass git hooks or mute failing checks.
- Use non-interactive flags (`cp -f`, `rm -f`, `ssh -o BatchMode=yes`) since
  shells may alias `-i`.
