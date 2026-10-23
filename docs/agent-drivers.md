# Gateway agent drivers

The authority runs OMP and T3 Code through `AgentDriver`. OMP keeps its existing
allowlisted environment, per-session uid allocation, MCP proxy, inline Linear
host tools, and credential cleanup. The compatibility adapter preserves OMP RPC
extension events while feeding both drivers into the existing lifecycle and
activity projector. T3 event identities remain stable across snapshot replays.

## Select a driver

Repository records have `defaultDriver`, either `omp` or `t3`. Existing rows
migrate to `omp`. The authenticated repository create/update API accepts this
field; omitting it on update preserves the value.

For each new run, the authority reads the repository record and the issue's
current Linear labels. `driver:t3` or `driver:omp` overrides the default. Matching
ignores case and surrounding whitespace. Conflicting overrides fail the run
attempt instead of depending on label order. No gateway restart is required.
A persisted session ref keeps its driver across retries and follow-up turns;
changing labels doesn't migrate an existing conversation between runtimes.

Set `T3_BASE_URL`, `T3_TOKEN` or `T3_TOKEN_FILE`, `T3_PROVIDER_INSTANCE`, and
`T3_MODEL` for T3. The token file is reread on connection so rotations take effect.
The sidecar contract from PR #15 uses `http://t3:3773` and a gateway-side token
file at `/run/secrets/t3-gateway-token`. Both containers must see a workspace at
the same absolute path. Provider instances such as `codex-cliproxy` and
`claude-cliproxy` belong to the sidecar and must route through CLIProxyAPI.

`T3_RUNTIME_MODE` defaults to `auto`. The authority answers any approval that
still escalates with `accept` once, inside the session's cancellation mutation
gate. Questions become Linear elicitations and replies return to the originating
request. A five-minute lack of progress, a held queue, or exhausted reconnects
interrupts the turn and enters the authority's existing bounded retry policy.
The watchdog pauses for pending user questions. Gateway recovery reattaches the
persisted thread, interrupts any old turn, and sends a continuation attempt.
Historical terminal events don't complete that new attempt. Completion waits for
queued turns before releasing the worker.

## Project provisioning

For a gateway `Directory` workspace, the driver looks up its active project with
`GET /api/projects` before launching the thread. If absent, it submits
`project.create` to `POST /api/projects/mutate`, with a deterministic project id
and command id. It requests no scripts and never asks T3 to create the directory.
A concurrent creation or lost mutation response triggers another lookup, so the
existing project is adopted rather than duplicated. `T3_PROJECT_ID` remains
optional for callers that use an existing project root or driver-created worktree.

These shapes were checked against T3 **v0.1.2**, commit **044f0725**, in
[`packages/contracts/src/project.ts`](https://github.com/pingdotgg/t3code/blob/v0.1.2/packages/contracts/src/project.ts),
[`packages/contracts/src/environmentHttp.ts`](https://github.com/pingdotgg/t3code/blob/v0.1.2/packages/contracts/src/environmentHttp.ts),
and [`apps/server/src/project/http.ts`](https://github.com/pingdotgg/t3code/blob/v0.1.2/apps/server/src/project/http.ts).
The API needs only `orchestration:read` and `orchestration:operate` scopes.

## Deployment gaps

Main's Docker workers have distinct uid/gid pairs and mode-0700 workspace roots.
PR #15 currently defines one shared fixed-uid T3 sidecar. That process can't
access all those workspaces while preserving session isolation. The gateway
therefore rejects T3 execution when worker uid isolation is configured, before
project creation or thread launch. It doesn't chmod workspaces, collapse the
uid range, or grant the sidecar root access. A per-session provider/sidecar
launcher contract is required before enabling T3 in that deployment.

T3 startup removes gateway-generated repository Git credential headers and fails
if that removal fails. It sends no GitHub, Linear, or upstream MCP tokens to T3.
The sidecar owns only its CLIProxyAPI client credential and scoped gateway-to-T3
auth token. Per-session Linear MCP tools and a scoped Git/PR credential broker
remain separate work. T3 runs without those tools can project plan, action,
response and elicitation events, but cannot use the OMP inline host-tool bridge.

The in-process T3 HTTP/WebSocket tests prove gateway behavior against the pinned
contract. They don't establish live sidecar compatibility or the deployed
Linear-session-to-PR acceptance target.
