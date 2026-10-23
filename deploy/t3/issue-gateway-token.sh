#!/usr/bin/env bash
# Mints the gateway's T3 bearer session and writes it to a 0400 file the
# gateway mounts as a _FILE secret. Run it inside the sidecar container, next
# to the server's data dir:
#
#   docker compose exec t3 t3-issue-gateway-token [--ttl 30d] [--output PATH]
#
# The session carries orchestration:read and orchestration:operate only, which
# covers the WS RPC orchestration methods (launchThread, dispatchCommand,
# subscribeThread, server.getConfig). It is not an mcp-client session, so T3
# rejects it on /mcp by design. Non-secret metadata (session id, expiry) goes
# to <output>.json so a later rotation can revoke the previous session.
set -euo pipefail

ttl="30d"
output="${T3_GATEWAY_TOKEN_FILE:-/data/gateway-auth/t3-gateway-token}"
label="ohmypi-symphony gateway"

usage() {
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
  echo
  echo "Flags: --ttl <duration> (default 30d), --output <path>, --label <text>"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ttl) ttl="${2:?--ttl needs a value}"; shift 2 ;;
    --output) output="${2:?--output needs a value}"; shift 2 ;;
    --label) label="${2:?--label needs a value}"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) echo "t3-issue-gateway-token: unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

: "${T3CODE_HOME:?T3CODE_HOME must be set}"
metadata="${output}.json"
previous_session=""
if [[ -s "$metadata" ]]; then
  previous_session="$(jq -r '.sessionId // empty' "$metadata")"
fi

umask 077
mkdir -p "$(dirname "$output")"
issued="$(t3 auth session issue \
  --base-dir "$T3CODE_HOME" \
  --scope orchestration:read \
  --scope orchestration:operate \
  --ttl "$ttl" \
  --label "$label" \
  --json)"

token="$(jq -r '.token // empty' <<<"$issued")"
[[ -n "$token" ]] || {
  echo "t3-issue-gateway-token: t3 auth session issue returned no token" >&2
  exit 1
}

tmp="$(mktemp "${output}.XXXXXX")"
printf '%s' "$token" >"$tmp"
chmod 0400 "$tmp"
mv -f "$tmp" "$output"
jq 'del(.token)' <<<"$issued" >"${metadata}.tmp"
chmod 0444 "${metadata}.tmp"
mv -f "${metadata}.tmp" "$metadata"

echo "Wrote ${output} (0400). Session metadata:"
cat "$metadata"
if [[ -n "$previous_session" ]]; then
  echo
  echo "Previous session ${previous_session} is still valid. Once the gateway has"
  echo "restarted with the new token, revoke it with:"
  echo "  t3 auth session revoke ${previous_session}"
fi
