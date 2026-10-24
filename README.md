# Partitura

Partitura connects Linear agent sessions to coding agents in repository workspaces.
It runs OMP workers or per-session T3 Code instances, with T3 providers for Claude
and Codex. It stores deliveries and run state in SQLite, recovers interrupted work,
and reports progress and results to Linear.

Partitura is Symphony-inspired. It doesn't implement the OpenAI Symphony Draft v1
contract: it uses Linear webhooks and agent sessions rather than a `WORKFLOW.md`
loader, the specified tracker polling scheduler, and a direct Codex app-server
runner.

## Run locally

Tool versions are pinned in `mise.toml`. Copy the settings described in
[.env.example](.env.example) into your environment and provision credentials through
the project's encrypted fnox configuration.

```sh
mise run install
mise run dev
```

Run `mise exec -- bun run check` for typechecking, lint, tests, and a production
build. See [AGENTS.md](AGENTS.md) for the architecture and contributor workflow.

## Operating docs

- [Agent drivers](docs/agent-drivers.md) covers OMP and T3 Code configuration.
- [Worker profile](docs/worker-profile.md) covers tools, providers, and credentials.
- [Security](docs/security.md) covers tenant admission, worker isolation, and state.

## Rename compatibility

This project was previously `ohmypi-symphony`. The GitHub repository is now
[`ramarivera/partitura`](https://github.com/ramarivera/partitura). Existing deployment
paths, the public origin, OAuth callbacks, app IDs, named volumes, database and
encryption keys remain valid. Existing deployments retain the `symphony` T3 ID
namespace and `omp_gateway_admin` cookie so the rename doesn't create new sessions
or lose existing state. The OMP runtime and its configuration still use their own
names.
