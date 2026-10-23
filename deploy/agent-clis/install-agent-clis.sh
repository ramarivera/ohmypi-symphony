#!/bin/sh
# Installs pinned, checksum-verified agent CLIs under /opt for the gateway
# image: Claude Code for OMP workers, and T3 Code with the Codex CLI it drives
# for the per-session T3 launcher. One set of release checksums.
#
#   install-agent-clis.sh claude=<version> [codex=<version>] [t3=<version>]
#
# Claude Code lands in /opt/claude/bin/claude, Codex in /opt/codex/bin/codex,
# T3 in /opt/t3/t3. Checksums: Claude from the release's manifest.json in
# Anthropic's distribution bucket (what the official installer verifies),
# Codex from the values T3 0.1.2 pins in
# apps/server/src/provider/CodexInstallation.ts, T3 from the GitHub release's
# SHA256SUMS asset. Adding a version means adding both architectures below.
set -eu

arch=${TARGETARCH:-$(dpkg --print-architecture)}
case "$arch" in
  amd64 | arm64) ;;
  *) echo "install-agent-clis: unsupported architecture $arch" >&2; exit 1 ;;
esac

checksum() {
  case "$1-$2-$arch" in
    claude-2.1.304-amd64) echo ea0f135784631619a7a409d07322ecec964f9cc4fd377508ed1ba3f141f27912 ;;
    claude-2.1.304-arm64) echo c487117526509ab6d2ee6013e14f181faf68d6d6afe6bb0d4b2b4a9feca5ecb3 ;;
    codex-0.156.1-amd64) echo 8b711520beddf385467b8da4d2c93736637c6ba1e46811cf0d8606b7c490b6f6 ;;
    codex-0.156.1-arm64) echo fdd47ed6aade0360796fd3f6f95a45096f327c15e19e8c7339f9dc5633041786 ;;
    t3-0.1.2-amd64) echo 7aed625cb961938df174c1a067c0f05235360940b53e882a1643126b754da16c ;;
    t3-0.1.2-arm64) echo df112e1f19772b4055a28c97b032eb6d038d5d7182015f1cafd3254ad6fabd0f ;;
    *) echo "install-agent-clis: no pinned checksum for $1 $2 on $arch" >&2; return 1 ;;
  esac
}

install_claude() {
  sum=$(checksum claude "$1")
  platform=linux-x64
  [ "$arch" = arm64 ] && platform=linux-arm64
  mkdir -p /opt/claude/bin
  curl --fail --location --retry 3 --output /opt/claude/bin/claude \
    "https://storage.googleapis.com/claude-code-dist-86c565f3-f756-42ad-8dfa-d59b1c096819/claude-code-releases/$1/${platform}/claude"
  echo "${sum}  /opt/claude/bin/claude" | sha256sum --check --strict
  chmod 0755 /opt/claude/bin/claude
}

install_codex() {
  sum=$(checksum codex "$1")
  target=x86_64-unknown-linux-musl
  [ "$arch" = arm64 ] && target=aarch64-unknown-linux-musl
  curl --fail --location --retry 3 --output /tmp/codex.tar.gz \
    "https://github.com/openai/codex/releases/download/rust-v$1/codex-package-${target}.tar.gz"
  echo "${sum}  /tmp/codex.tar.gz" | sha256sum --check --strict
  mkdir -p /opt/codex
  tar -xzf /tmp/codex.tar.gz -C /opt/codex
  test -x /opt/codex/bin/codex
  rm -f /tmp/codex.tar.gz
}

install_t3() {
  sum=$(checksum t3 "$1")
  platform=linux-x64
  [ "$arch" = arm64 ] && platform=linux-arm64
  curl --fail --location --retry 3 --output /tmp/t3.tar.gz \
    "https://github.com/pingdotgg/t3code/releases/download/v$1/t3-$1-${platform}.tar.gz"
  echo "${sum}  /tmp/t3.tar.gz" | sha256sum --check --strict
  mkdir -p /opt/t3
  tar -xzf /tmp/t3.tar.gz -C /opt/t3 --strip-components=1
  test -x /opt/t3/t3
  rm -f /tmp/t3.tar.gz
}

[ $# -gt 0 ] || { echo "usage: install-agent-clis.sh claude=<version> [codex=<version>] [t3=<version>]" >&2; exit 64; }
for spec in "$@"; do
  version=${spec#*=}
  case "$spec" in
    claude=?*) install_claude "$version" ;;
    codex=?*) install_codex "$version" ;;
    t3=?*) install_t3 "$version" ;;
    *) echo "install-agent-clis: unknown component $spec" >&2; exit 64 ;;
  esac
done
