# Worker omp profile and model routing

Hardened workers run with a private `PI_CODING_AGENT_DIR` at
`<workspace>/.omp-gateway`, so they never see the host's `~/.omp/agent`. The
gateway gives them two things instead: a curated, credential-free profile, and
one revocable CLIProxyAPI client key. Every model call goes through
CLIProxyAPI. omp itself gets no Anthropic endpoint or credential, so its
built-in Anthropic provider is never available to a worker.

## Profile contract

Mount the profile read-only and set `WORKER_PROFILE_DIR` to it. The image
creates an empty `/app/worker-profile` mount point for this. Allowed top-level
entries:

| Entry | Kind |
| --- | --- |
| `skills/`, `extensions/`, `rules/`, `agents/`, `themes/` | directories |
| `models.yml`, `config.yml` | files |
| Uppercase context files such as `AGENTS.md`, `RULES.md`, `SYSTEM.md`, `APPEND_SYSTEM.md` | files |

Any other top-level name fails the run start. The gateway validates the mount
before every worker start, because the mount can change without a restart:

- The source path must be canonical. Every entry must be a regular file or
  directory. Symlinks anywhere fail.
- Every inode must belong to root or the gateway uid and have no group or other
  write bit. With per-session uids, files must be other-readable and
  directories other-searchable, because the copy runs as the worker uid.
- Credential and state names fail anywhere in the tree: `auth.json`,
  `credentials.json`, `mcp.json`, `.env` and `.env.*` (except `.env.example`),
  `.netrc`, `.npmrc`, `.pypirc`, SSH private keys, `sessions`, and any
  `*.db`, `*.db-wal`, `*.db-shm`, `*.db-journal`, `*.sqlite`, `*.sqlite3`,
  `*.pem`, `*.key`, `*.p12`, `*.pfx` or `*.token` file. So `agent.db`,
  `history.db`, session trees and key material never reach a worker.
- `config.yml` must not contain credential-named keys (`apiKey`, `token`,
  `secret`, `password`, `authorization`, `headers` and similar) or omp `!command`
  values.
- `models.yml` may only set `providers`. Each provider must use
  `api: openai-responses` or `openai-completions` with a `baseUrl` under
  `CLIPROXYAPI_BASE_URL`, and may only reference the key by name:
  `apiKey: CLIPROXYAPI_API_KEY`. Literal keys, `headers`, `auth`, `transport`,
  `anthropic-messages` providers and a `pi-claude-cli` override all fail.
- `WORKER_DEFAULT_MODEL`'s provider must be declared in `models.yml` or be
  `pi-claude-cli`.
- `WORKER_PROFILE_MAX_BYTES` (default 64 MiB) and `WORKER_PROFILE_MAX_ENTRIES`
  (default 10,000) cap the tree.

The worker-uid helper then copies only the validated manifest into
`.omp-gateway`. It opens every source file without following links, rechecks
type and size against the manifest, and creates destination files exclusively
(mode 0600, or 0700 for executables). Profile-managed entries are replaced on
each start. `agent.db`, `mcp.json` and session files in the agent dir are left
alone. Profile extensions run as the worker and can't resolve packages outside
their own files; install dependency-bearing extensions into the image instead.

Build the profile from the host `~/.omp/agent` by copying `skills/`, `rules/`,
`extensions/`, `agents/`, the context files, and a `models.yml` rewritten to the
CLIProxyAPI provider below. Never copy `agent.db`, `auth.json`, `sessions/` or
`history.db`.

```yaml
# models.yml
providers:
  cliproxyapi:
    api: openai-responses
    baseUrl: https://cliproxyapi.ai.roxasroot.net/v1
    apiKey: CLIPROXYAPI_API_KEY
    models:
      - id: gpt-6-sol
        name: GPT-6 Sol (CLIProxyAPI)
```

## Model routing

| Setting | Meaning |
| --- | --- |
| `CLIPROXYAPI_BASE_URL` | CLIProxyAPI origin, http(s) without credentials, query or fragment. |
| `CLIPROXYAPI_API_KEY` or `CLIPROXYAPI_API_KEY_FILE` | Dedicated, revocable Symphony client key. In Docker, a root-owned mode-0400 secret. Both settings are required together. |
| `WORKER_DEFAULT_MODEL` | `provider/model[:thinking]`. Defaults to `cliproxyapi/gpt-6-sol` when `WORKER_PROFILE_DIR` and CLIProxyAPI are configured. Native/dev runs without a profile leave omp's default unchanged. Passed as `--model` to fresh sessions only; resumed sessions keep their saved model. |
| `WORKER_EXTENSION_PATHS` | Comma-separated absolute paths of image-installed extensions loaded into every worker. The image sets pi-claude-cli. |
| `WORKER_CLAUDE_EXECUTABLE` | Claude Code launcher for pi-claude-cli. The image sets `/usr/local/bin/claude-cliproxy`. |

The repository's encrypted fnox entry is named
`SYMPHONY_DEPLOY_CLIPROXYAPI_API_KEY`. Toolbox pre-up maps that value to the
`CLIPROXYAPI_API_KEY` Docker secret file. Native `mise run start` and
`mise run dev` don't map it, so they can start without model routing. To enable
routing natively, explicitly supply both runtime settings above.

Codex and OpenAI models use the `models.yml` provider, so omp calls
`<CLIPROXYAPI_BASE_URL>/v1/responses` with the client key as a bearer token.

Claude models use `pi-claude-cli/<model>`. The gateway runs pi-claude-cli with
`PI_CLAUDE_DRIVER=cli`, `PI_CLAUDE_AUTH=claude-login` and
`PI_CLAUDE_EXECUTABLE=claude-cliproxy`. In claude-login mode pi-claude-cli
refuses to start if omp's environment carries any `ANTHROPIC_*` or Claude Code
auth override, which also proves omp has none. `claude-cliproxy` maps
`CLIPROXYAPI_BASE_URL` and the key to `ANTHROPIC_BASE_URL` and
`ANTHROPIC_AUTH_TOKEN` for the Claude Code child only, then drops the
CLIProxyAPI variable and every other auth source.

The worker receives exactly `CLIPROXYAPI_BASE_URL`, `CLIPROXYAPI_API_KEY` and the
three `PI_CLAUDE_*` values, injected per run after the environment allowlist.
The allowlist itself forwards no model-provider keys or endpoints, including a
copy of the CLIProxyAPI key in the gateway's own environment.

## Image

`deploy/agent-clis/install-agent-clis.sh` installs pinned, checksum-verified
Claude Code, Codex and T3 builds under `/opt`. The gateway image installs
Claude Code 2.1.304, Codex 0.156.1 and T3 0.1.2; launched T3 daemons drive the
same Claude Code build through `claude-cliproxy`. pi-claude-cli 0.5.1 supports Claude Code
`>=2.1.285 <2.2.0`, Pi `>=0.99.1 <0.100.0`, and omp `^18.8.6`. Claude Code
versions above the supported range warn by default;
`PI_CLAUDE_CLI_STRICT_VERSION=1` makes that check strict. `deploy/worker-extensions`
pins pi-claude-cli with its own `bun.lock`; it's installed with
`--omit=optional --ignore-scripts`, so the Agent SDK's bundled Claude binaries
are skipped.

Upgrading pi-claude-cli means checking its omp peer range against
`OMP_VERSION` and its supported Claude Code range against `CLAUDE_VERSION`,
then adding both architectures' checksums to the installer.
