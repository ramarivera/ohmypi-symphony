# omp 18.8.6 is the release pi-claude-cli 0.5.0 targets and was verified on.
ARG OMP_VERSION=18.8.6
# pi-claude-cli's CLI driver refuses any other Claude Code build.
ARG CLAUDE_VERSION=2.1.285

FROM oven/bun:1.3.14 AS omp
ARG OMP_VERSION
ARG TARGETARCH
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETARCH" in \
      amd64) asset="omp-linux-x64"; checksum="877acdc48384b80fe4c083b1e610433d28922dd9fccde9ea430aaace8765b19b" ;; \
      arm64) asset="omp-linux-arm64"; checksum="a6bf1405877a739965586fcf973136f72078d2e6dc288905fab08588469c13cf" ;; \
      *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
  && curl --fail --location --retry 3 \
    "https://github.com/can1357/oh-my-pi/releases/download/v${OMP_VERSION}/${asset}" \
    --output /usr/local/bin/omp \
  && echo "${checksum}  /usr/local/bin/omp" | sha256sum --check --strict \
  && chmod 0755 /usr/local/bin/omp

# Claude Code for pi-claude-cli, via the installer shared with deploy/t3.
FROM oven/bun:1.3.14 AS agent-clis
ARG CLAUDE_VERSION
ARG TARGETARCH
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
COPY deploy/agent-clis/install-agent-clis.sh /usr/local/bin/install-agent-clis
RUN TARGETARCH="$TARGETARCH" install-agent-clis "claude=${CLAUDE_VERSION}"

# Worker omp extensions, pinned by bun.lock. The Agent SDK's optional bundled
# Claude binaries are skipped: workers use the CLI driver and /opt/claude.
FROM oven/bun:1.3.14 AS worker-extensions
WORKDIR /opt/omp-extensions
COPY deploy/worker-extensions/package.json deploy/worker-extensions/bun.lock ./
RUN bun install --frozen-lockfile --production --omit=optional --ignore-scripts

FROM oven/bun:1.3.14 AS build
WORKDIR /app
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile
COPY tsconfig.json biome.json ./
COPY src ./src
RUN bun run build

FROM oven/bun:1.3.14
WORKDIR /app
ENV NODE_ENV=production
ENV PATH="/opt/nix/bin:/app/node_modules/.bin:${PATH}"
ENV NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
# Install a pinned single-user Nix while building the image. Docker copies the
# populated /nix into gateway-nix when it initializes the named volume.
# Share config and binaries without traversing bun's private HOME. Leave store
# ownership to the installer; the gateway prepares packages as root.
RUN apt-get update \
  && apt-get install -y --no-install-recommends bash ca-certificates curl git openssh-client xz-utils util-linux \
  && install -d -m 0755 -o bun -g bun /nix /app/data /app/logs /app/nix-roots /app/workspaces /home/bun/.omp/natives \
  && curl --fail --location --retry 3 --output /tmp/nix-install https://releases.nixos.org/nix/nix-2.30.3/install \
  && echo "8ff029ac2a49134441dc14c9168abb04506710834d7390039a8c1800dd998cd9  /tmp/nix-install" | sha256sum --check --strict \
  && chmod 0755 /tmp/nix-install \
  && su -s /bin/sh bun -c 'HOME=/home/bun /tmp/nix-install --no-daemon' \
  && rm -f /tmp/nix-install \
  && install -d -m 0755 /etc/nix \
  && printf '%s\n' 'experimental-features = nix-command flakes' 'sandbox = false' 'build-users-group =' > /etc/nix/nix.conf \
  && ln -s "$(readlink -f /home/bun/.nix-profile)" /opt/nix \
  && apt-get purge -y --auto-remove curl xz-utils \
  && rm -rf /var/lib/apt/lists/*
COPY --from=omp /usr/local/bin/omp /usr/local/bin/omp
COPY --from=agent-clis /opt/claude /opt/claude
COPY --from=worker-extensions /opt/omp-extensions /opt/omp-extensions
COPY deploy/agent-clis/claude-cliproxy /usr/local/bin/claude-cliproxy
RUN ln -s /opt/claude/bin/claude /usr/local/bin/claude \
  && chown -Rh root:root /opt/claude /opt/omp-extensions /usr/local/bin/claude-cliproxy \
  && chmod -R go-w /opt/claude /opt/omp-extensions \
  && chmod 0755 /usr/local/bin/claude-cliproxy \
  && install -d -m 0755 -o root -g root /app/worker-profile
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json /app/bun.lock* ./
RUN bun install --production --frozen-lockfile
# The gateway owns shared executable inputs. Workers use numeric session ids
# with no capabilities or privilege escalation; no passwd entry is required.
RUN chown -Rh root:root /home/bun /app \
  && chmod -R go-w /home/bun /app \
  && chown root:root /app/data /app/logs /app/nix-roots /app/workspaces \
  && chmod 0700 /app/data /app/logs \
  && chmod 0755 /app/nix-roots /app/workspaces
COPY docker-entrypoint.sh /usr/local/bin/gateway-entrypoint
RUN chmod 0755 /usr/local/bin/gateway-entrypoint
# Entrypoint defaults to a 64-slot range, or one slot for legacy WORKER_UID.
ENV HOME=/root
ENV WORKSPACE_ROOT=/app/workspaces
# Every worker loads pi-claude-cli; Claude models route through claude-cliproxy.
# Mount a curated profile at /app/worker-profile and set WORKER_PROFILE_DIR.
ENV WORKER_EXTENSION_PATHS=/opt/omp-extensions/node_modules/@ramarivera/pi-claude-cli/entrypoints/omp.ts
ENV WORKER_CLAUDE_EXECUTABLE=/usr/local/bin/claude-cliproxy
ENV DISABLE_AUTOUPDATER=1
USER root
ENTRYPOINT ["/usr/local/bin/gateway-entrypoint"]
EXPOSE 3000
CMD ["bun", "dist/index.js"]
