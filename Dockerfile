ARG OMP_VERSION=17.1.8

FROM oven/bun:1.3.14 AS omp
ARG OMP_VERSION
ARG TARGETARCH
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETARCH" in \
      amd64) asset="omp-linux-x64"; checksum="7ee37fa2acdc461fe286f767e75393a7bac2500ff6383b863714121f73d610e4" ;; \
      arm64) asset="omp-linux-arm64"; checksum="c2d79e2e4d665b54bbdcf7a174a892b0346ce1b63a4ad5f8bfe95c5ec828bb66" ;; \
      *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
  && curl --fail --location --retry 3 \
    "https://github.com/can1357/oh-my-pi/releases/download/v${OMP_VERSION}/${asset}" \
    --output /usr/local/bin/omp \
  && echo "${checksum}  /usr/local/bin/omp" | sha256sum --check --strict \
  && chmod 0755 /usr/local/bin/omp

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
ENV PATH="/home/bun/.nix-profile/bin:/app/node_modules/.bin:${PATH}"
ENV NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
# Install a pinned single-user Nix while building the image. Docker copies the
# populated /nix into gateway-nix when it initializes the named volume.
RUN apt-get update \
  && apt-get install -y --no-install-recommends bash ca-certificates curl git openssh-client xz-utils \
  && install -d -m 0755 -o bun -g bun /nix /app/data /app/logs /app/nix-roots /app/workspaces /home/bun/.omp/natives /home/bun/.config/nix \
  && curl --fail --location --retry 3 --output /tmp/nix-install https://releases.nixos.org/nix/nix-2.30.3/install \
  && echo "8ff029ac2a49134441dc14c9168abb04506710834d7390039a8c1800dd998cd9  /tmp/nix-install" | sha256sum --check --strict \
  && chmod 0755 /tmp/nix-install \
  && su -s /bin/sh bun -c 'HOME=/home/bun /tmp/nix-install --no-daemon' \
  && rm -f /tmp/nix-install \
  && printf '%s\n' 'experimental-features = nix-command flakes' 'sandbox = false' > /home/bun/.config/nix/nix.conf \
  && chown -R bun:bun /app /home/bun /nix \
  && apt-get purge -y --auto-remove curl xz-utils \
  && rm -rf /var/lib/apt/lists/*
COPY --from=omp /usr/local/bin/omp /usr/local/bin/omp
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json /app/bun.lock* ./
RUN bun install --production --frozen-lockfile
USER bun
EXPOSE 3000
CMD ["bun", "dist/index.js"]
