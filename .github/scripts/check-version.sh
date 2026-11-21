#!/usr/bin/env bash
# Fails unless every named crate's version equals the version in the tag.
#
# usage: check-version.sh <tag prefix> <crate>...
#   e.g. check-version.sh client-v rasterizeddb_client
#
# With ALLOW_PRERELEASE=1, a pre-release suffix on the tag is set aside before
# comparing: v1.0.0-rc.1 matches crates at 1.0.0. Only the executables channel
# sets it; a crate tag must name exactly the version crates.io will receive.
set -euo pipefail
prefix="$1"; shift
tag="${GITHUB_REF_NAME:?not running on a tag}"
want="${tag#"$prefix"}"
if [ "$want" = "$tag" ]; then
  echo "::error::tag $tag does not start with $prefix"; exit 1
fi
if [ "${ALLOW_PRERELEASE:-0}" = 1 ]; then want="${want%%-*}"; fi
crate_version() {
  # `cargo pkgid` ends in `#name@version` (or `#version`).
  cargo pkgid -p "$1" | sed 's/.*[#@]//'
}
status=0
for crate in "$@"; do
  have="$(crate_version "$crate")"
  if [ "$have" != "$want" ]; then
    echo "::error::$crate is $have in Cargo.toml but the tag says $want"
    status=1
  else
    echo "$crate $have matches $tag"
  fi
done
exit $status
