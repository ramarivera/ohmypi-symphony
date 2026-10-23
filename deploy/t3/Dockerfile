# T3 Code sidecar for the gateway's T3 driver. It runs `t3 serve` with the
# Codex and Claude CLIs underneath, both pointed at CLIProxyAPI by the
# entrypoint. Build context is the repository root:
#   docker build -f deploy/t3/Dockerfile -t ohmypi-symphony-t3 .
# Pin and upgrade procedure: docs/t3-driver-deploy.md.

# t3 standalone server, from the stable GitHub release (SHA256SUMS asset).
ARG T3_VERSION=0.1.2
# The Codex build T3 0.1.2 itself pins for managed installs
# (apps/server/src/provider/CodexInstallation.ts at v0.1.2).
ARG CODEX_VERSION=0.156.1
# Same Claude Code build as workbench and arch-cachy on 2026-10-23.
ARG CLAUDE_VERSION=2.1.304

FROM debian:trixie-slim AS fetch
ARG T3_VERSION
ARG CODEX_VERSION
ARG CLAUDE_VERSION
ARG TARGETARCH
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETARCH" in \
      amd64) t3_asset="t3-${T3_VERSION}-linux-x64"; t3_sum="7aed625cb961938df174c1a067c0f05235360940b53e882a1643126b754da16c" ;; \
      arm64) t3_asset="t3-${T3_VERSION}-linux-arm64"; t3_sum="df112e1f19772b4055a28c97b032eb6d038d5d7182015f1cafd3254ad6fabd0f" ;; \
      *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
  && curl --fail --location --retry 3 --output /tmp/t3.tar.gz \
    "https://github.com/pingdotgg/t3code/releases/download/v${T3_VERSION}/${t3_asset}.tar.gz" \
  && echo "${t3_sum}  /tmp/t3.tar.gz" | sha256sum --check --strict \
  && mkdir -p /opt/t3 \
  && tar -xzf /tmp/t3.tar.gz -C /opt/t3 --strip-components=1 \
  && test -x /opt/t3/t3 \
  && rm -f /tmp/t3.tar.gz
# One release/checksum source shared with the gateway image.
COPY deploy/agent-clis/install-agent-clis.sh /usr/local/bin/install-agent-clis
RUN TARGETARCH="$TARGETARCH" sh /usr/local/bin/install-agent-clis "claude=${CLAUDE_VERSION}" "codex=${CODEX_VERSION}"

FROM debian:trixie-slim
ARG T3_VERSION
ARG CODEX_VERSION
ARG CLAUDE_VERSION
# libatomic1 is the one shared library the t3 binary needs beyond glibc. git,
# ssh, and ripgrep are for the agents' own tool calls; jq renders the
# generated config; tini reaps the agents' orphaned children.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash ca-certificates curl git jq libatomic1 openssh-client procps ripgrep tini \
  && rm -rf /var/lib/apt/lists/*
# Dedicated T3 trust domain, outside gateway worker slots 1001..1064.
# The gateway must explicitly grant a T3 workspace; no blanket chmod/chown.
RUN groupadd --gid 2001 t3 \
  && useradd --uid 2001 --gid 2001 --create-home --home-dir /home/t3 --shell /bin/bash t3 \
  && install -d -m 0750 -o t3 -g t3 /data /data/t3 /data/gateway-auth \
  && install -d -m 0755 -o t3 -g t3 /app /app/workspaces
COPY --from=fetch /opt/t3 /opt/t3
COPY --from=fetch /opt/codex /opt/codex
COPY --from=fetch /opt/claude /opt/claude
COPY --chmod=0755 deploy/agent-clis/claude-cliproxy /usr/local/bin/claude-cliproxy
RUN ln -s /opt/t3/t3 /usr/local/bin/t3 \
  && ln -s /opt/codex/bin/codex /usr/local/bin/codex \
  && ln -s /usr/local/bin/claude-cliproxy /usr/local/bin/claude
COPY --chmod=0755 deploy/t3/docker-entrypoint.sh /usr/local/bin/t3-sidecar-entrypoint
COPY --chmod=0755 deploy/t3/issue-gateway-token.sh /usr/local/bin/t3-issue-gateway-token

ENV HOME=/home/t3 \
  T3CODE_HOME=/data/t3 \
  T3CODE_HOST=0.0.0.0 \
  T3CODE_PORT=3773 \
  T3_SIDECAR_VERSION_T3=${T3_VERSION} \
  T3_SIDECAR_VERSION_CODEX=${CODEX_VERSION} \
  T3_SIDECAR_VERSION_CLAUDE=${CLAUDE_VERSION} \
  DISABLE_AUTOUPDATER=1
USER t3
WORKDIR /app/workspaces
VOLUME ["/data/t3", "/data/gateway-auth"]
EXPOSE 3773
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl --fail --silent --output /dev/null "http://127.0.0.1:${T3CODE_PORT}/.well-known/t3/environment" || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/t3-sidecar-entrypoint"]
CMD ["serve"]
