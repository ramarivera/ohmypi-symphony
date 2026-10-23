#!/bin/sh
set -eu
case "${WORKER_UID:-}" in
  ""|0|*[!0-9]*) echo "WORKER_UID must be a non-root numeric uid" >&2; exit 1 ;;
esac
# Named volumes may predate uid isolation. Lock gateway state before startup.
chown root:root /app/data /app/logs
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
# Keep only filesystem ownership/access and worker identity transition powers.
# SETPCAP is required when setpriv drops every worker bounding capability.
exec setpriv --bounding-set=-all,+chown,+dac_override,+setgid,+setuid,+setpcap -- "$@"
