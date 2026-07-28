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
ENV PATH="/app/node_modules/.bin:${PATH}"
RUN apt-get update && apt-get install -y --no-install-recommends bash ca-certificates git openssh-client \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json /app/bun.lock* ./
RUN bun install --production --frozen-lockfile
RUN mkdir -p /app/data && chown -R bun:bun /app
USER bun
EXPOSE 3000
CMD ["bun", "dist/index.js"]
