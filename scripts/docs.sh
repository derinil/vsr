#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="$root/build/docs"
packages=(vsr vsrclient vsrreplica vsrtcp kvservice cmd/vsrd cmd/vsrctl cmd/vsrmaelstrom)

usage() {
	echo "usage: $0 [serve|gen]" >&2
	echo "  serve  browse HTML docs locally (default)" >&2
	echo "  gen    write text docs to build/docs" >&2
	exit 2
}

gen() {
	rm -rf "$out"
	mkdir -p "$out"
	for p in "${packages[@]}"; do
		f="$out/${p//\//-}.txt"
		(cd "$root" && go doc -all "./$p") >"$f"
		echo "docs: $f" >&2
	done
}

serve() {
	cd "$root/vsr"
	exec go doc -http
}

case "${1:-serve}" in
serve) serve ;;
gen) gen ;;
*) usage ;;
esac
