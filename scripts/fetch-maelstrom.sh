#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest="$root/build/maelstrom"
fallback_url="https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2"

if [[ -x "$dest/maelstrom" ]]; then
  exit 0
fi

url="$(curl -fsSL https://api.github.com/repos/jepsen-io/maelstrom/releases/latest 2>/dev/null \
  | sed -n 's/.*"browser_download_url": *"\([^"]*maelstrom\.tar\.bz2\)".*/\1/p' \
  | head -n 1)" || url=""
url="${url:-$fallback_url}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $url"
curl -fsSL "$url" -o "$tmp/maelstrom.tar.bz2"
tar -xjf "$tmp/maelstrom.tar.bz2" -C "$tmp"

rm -rf "$dest"
mkdir -p "$root/build"
mv "$tmp/maelstrom" "$dest"
chmod +x "$dest/maelstrom"
echo "Maelstrom installed at $dest"
