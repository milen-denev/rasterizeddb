#!/usr/bin/env bash
# Publishes workspace crates to crates.io in the order given (dependencies
# first), skipping any whose version is already there, so a release that
# failed halfway can simply be re-run.
#
# usage: publish-crates.sh [--dry-run] <crate>...
# env:   CARGO_REGISTRY_TOKEN (not needed for --dry-run)
set -euo pipefail

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then DRY_RUN=1; shift; fi

crate_version() {
  # `cargo pkgid` ends in `#name@version` (or `#version`).
  cargo pkgid -p "$1" | sed 's/.*[#@]//'
}

on_crates_io() {
  # 200 when that exact version exists; crates.io asks automated clients to
  # identify themselves.
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    -A "rasterizeddb-release-workflow (github.com/${GITHUB_REPOSITORY:-milen-denev/rasterizeddb})" \
    "https://crates.io/api/v1/crates/$1/$2")"
  [ "$code" = "200" ]
}

for crate in "$@"; do
  version="$(crate_version "$crate" 2>/dev/null || true)"
  if [ -z "$version" ]; then
    echo "::error::no crate named $crate in this workspace"; exit 1
  fi
  if on_crates_io "$crate" "$version"; then
    echo "$crate $version is already on crates.io, skipped"
    continue
  fi
  if [ "$DRY_RUN" = 1 ]; then
    echo "== would publish $crate $version"
    cargo publish --locked --dry-run -p "$crate"
  else
    echo "== publishing $crate $version"
    # cargo waits until the index has it, so the next crate can depend on it.
    cargo publish --locked -p "$crate"
  fi
done
