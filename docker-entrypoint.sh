#!/bin/sh
set -eu
# WORKER_UID alone retains its one-slot legacy meaning. Explicit range wins.
WORKER_UID_COUNT=${WORKER_UID_COUNT:-$(if [ -n "${WORKER_UID_BASE:-}" ] || [ -z "${WORKER_UID:-}" ]; then echo 64; else echo 1; fi)}
WORKER_UID_BASE=${WORKER_UID_BASE:-${WORKER_UID:-1001}}
for value in "$WORKER_UID_BASE" "$WORKER_UID_COUNT"; do
  case "$value" in
    ""|0*|*[!0-9]*) echo "Worker uid base/count must be positive integers" >&2; exit 1 ;;
  esac
  if [ "${#value}" -gt 10 ]; then
    echo "Worker uid base/count exceeds Linux uid/gid values" >&2; exit 1
  fi
done
if [ "$WORKER_UID_BASE" -gt 4294967294 ] || [ "$WORKER_UID_COUNT" -gt 4294967294 ] ||
   [ "$((WORKER_UID_BASE + WORKER_UID_COUNT))" -gt 4294967295 ]; then
  echo "Worker uid range exceeds Linux uid/gid values" >&2; exit 1
fi
export WORKER_UID_BASE WORKER_UID_COUNT
unset WORKER_UID
# Legacy volumes were writable by bun or the old worker. Shared executable
# inputs belong to root, with no group/other writes; each worker has private caches.
for shared in /home/bun /app/node_modules /app/dist /app/nix-roots; do
  chown -Rh root:root "$shared"
  chmod -R go-w "$shared"
done
# Startup requires the previous gateway/workers stopped. Seal legacy roots before
# any uid can be allocated; otherwise an unresumed old session could be exposed.
workspace_root=${WORKSPACE_ROOT:-/app/workspaces}
mkdir -p "$workspace_root"
chown root:root "$workspace_root"
chmod 0755 "$workspace_root"
find "$workspace_root" -mindepth 1 -maxdepth 1 -type d -exec chown root:root {} + -exec chmod 0700 {} +
# Named volumes may predate uid isolation. Lock gateway state before startup.
for path in /app/data /app/logs; do
  if [ -L "$path" ] || [ ! -d "$path" ]; then
    echo "$path must be a real directory" >&2
    exit 1
  fi
done
chown -h root:root /app/data /app/logs
chmod 0700 /app/data /app/logs
# Docker secrets must be mounted root-owned with mode 0400. Reject insecure
# mounts instead of assuming a readonly secret's mode can be changed.
for name in LINEAR_CLIENT_ID LINEAR_CLIENT_SECRET LINEAR_WEBHOOK_SECRET TOKEN_ENCRYPTION_KEY TOKEN_ENCRYPTION_PREVIOUS_KEYS_JSON GITHUB_APP_PRIVATE_KEY; do
  path=$(printenv "${name}_FILE" || true)
  if [ -n "$path" ]; then
    owner=$(stat -c '%u' "$path")
    mode=$(stat -c '%a' "$path")
    if [ "$owner" != 0 ] || [ "$mode" != 400 ]; then
      echo "${name}_FILE must be root-owned with mode 0400" >&2
      exit 1
    fi
  fi
done
# FOWNER is dropped below, so legacy bun-owned state must belong to the gateway
# before SQLite hardens its mode. Include custom DB paths, sidecars and nested
# state, but never traverse symlinks.
# Reject foreign-owned hardlinks rather than change an inode outside this tree.
if [ -n "$(find -P /app/data -type f -links +1 \( ! -uid 0 -o ! -gid 0 \) -print -quit)" ]; then
  echo "/app/data contains a legacy hardlink; copy it to a separate file before startup" >&2
  exit 1
fi
find -P /app/data \( -type d -o -type f \) \( ! -uid 0 -o ! -gid 0 \) -exec chown -h root:root {} +
# Keep filesystem access, worker identity transition, and worker signal delivery.
# SETPCAP is required when setpriv drops every worker bounding capability.
exec setpriv --bounding-set=-all,+chown,+dac_override,+kill,+setgid,+setuid,+setpcap -- "$@"
