FROM oven/bun:1.2.19-alpine AS build
WORKDIR /app
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile
COPY tsconfig.json biome.json ./
COPY src ./src
RUN bun run build

FROM oven/bun:1.2.19-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/dist ./dist
COPY --from=build /app/package.json /app/bun.lock* ./
RUN bun install --production --frozen-lockfile
USER bun
EXPOSE 3000
CMD ["bun", "dist/index.js"]
