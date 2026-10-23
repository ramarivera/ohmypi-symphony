# T3 Code sidecar for the gateway's T3 driver. It runs `t3 serve` with the
# Codex and Claude CLIs underneath, both pointed at CLIProxyAPI by the
# entrypoint. Build context is this directory:
#   docker build -f deploy/t3/Dockerfile -t ohmypi-symphony-t3 deploy/t3
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
RUN case "$TARGETARCH" in \
      amd64) codex_asset="codex-package-x86_64-unknown-linux-musl"; codex_sum="8b711520beddf385467b8da4d2c93736637c6ba1e46811cf0d8606b7c490b6f6" ;; \
      arm64) codex_asset="codex-package-aarch64-unknown-linux-musl"; codex_sum="fdd47ed6aade0360796fd3f6f95a45096f327c15e19e8c7339f9dc5633041786" ;; \
    esac \
  && curl --fail --location --retry 3 --output /tmp/codex.tar.gz \
    "https://github.com/openai/codex/releases/download/rust-v${CODEX_VERSION}/${codex_asset}.tar.gz" \
  && echo "${codex_sum}  /tmp/codex.tar.gz" | sha256sum --check --strict \
  && mkdir -p /opt/codex \
  && tar -xzf /tmp/codex.tar.gz -C /opt/codex \
  && test -x /opt/codex/bin/codex \
  && rm -f /tmp/codex.tar.gz
# Checksums come from the release's manifest.json in Anthropic's distribution
# bucket, the same source the official installer verifies against.
RUN case "$TARGETARCH" in \
      amd64) claude_platform="linux-x64"; claude_sum="ea0f135784631619a7a409d07322ecec964f9cc4fd377508ed1ba3f141f27912" ;; \
      arm64) claude_platform="linux-arm64"; claude_sum="c487117526509ab6d2ee6013e14f181faf68d6d6afe6bb0d4b2b4a9feca5ecb3" ;; \
    esac \
  && mkdir -p /opt/claude/bin \
  && curl --fail --location --retry 3 --output /opt/claude/bin/claude \
    "https://storage.googleapis.com/claude-code-dist-86c565f3-f756-42ad-8dfa-d59b1c096819/claude-code-releases/${CLAUDE_VERSION}/${claude_platform}/claude" \
  && echo "${claude_sum}  /opt/claude/bin/claude" | sha256sum --check --strict \
  && chmod 0755 /opt/claude/bin/claude

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
# uid/gid 1000 matches the gateway image's `bun` user, so both containers can
# write the shared /app/workspaces volume.
RUN groupadd --gid 1000 t3 \
  && useradd --uid 1000 --gid 1000 --create-home --home-dir /home/t3 --shell /bin/bash t3 \
  && install -d -m 0750 -o t3 -g t3 /data /data/t3 /data/gateway-auth \
  && install -d -m 0755 -o t3 -g t3 /app /app/workspaces
COPY --from=fetch /opt/t3 /opt/t3
COPY --from=fetch /opt/codex /opt/codex
COPY --from=fetch /opt/claude /opt/claude
RUN ln -s /opt/t3/t3 /usr/local/bin/t3 \
  && ln -s /opt/codex/bin/codex /usr/local/bin/codex \
  && ln -s /opt/claude/bin/claude /usr/local/bin/claude
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/t3-sidecar-entrypoint
COPY --chmod=0755 issue-gateway-token.sh /usr/local/bin/t3-issue-gateway-token

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
