#!/usr/bin/env bash
# Builds the release server with profile-guided optimisation (PGO) and, on
# Linux, BOLT on top. Each stage trains on pgo_train's workload against the
# real server binary (see pgo_train/src/main.rs).
#
#   1. build pgo_train (plain release)
#   2. build the server instrumented, run the workload, merge the profiles
#   3. build the server again using the merged profile
#   4. Linux only: instrument that binary with llvm-bolt, run the workload
#      again, and rewrite the binary's layout from what it recorded
#
# usage: pgo-build.sh <target triple> [--no-bolt]
#
# The result is target/<triple>/release/rasterizeddb_core[.exe], where a plain
# `cargo build --release --target <triple>` would leave it.
#
# The profiling flags go in through `--config target.<triple>.rustflags`, which
# cargo appends to the per-target flags in .cargo/config.toml (stack size,
# target-cpu). RUSTFLAGS would replace them.
#
# Needs: rustup's llvm-tools component (llvm-profdata matching rustc's LLVM);
# for BOLT, llvm-bolt on PATH or in $LLVM_BOLT.
set -euo pipefail

TARGET="${1:?usage: pgo-build.sh <target triple> [--no-bolt]}"
BOLT=1
if [ "${2:-}" = "--no-bolt" ]; then BOLT=0; fi
EXE=""
case "$TARGET" in
  *windows*) EXE=".exe"; BOLT=0 ;;  # BOLT rewrites ELF binaries only
  # The static musl binaries (Alpine image): PGO only, as BOLT's
  # instrumentation is untested here on a static-pie executable.
  *linux-musl*) BOLT=0 ;;
  *linux*) ;;
  *) BOLT=0 ;;
esac

# rustc on Windows wants a Windows path in -Cprofile-*.
native_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi
}

# Which server: the GPL one (rasterizeddb_core's binary) unless the caller
# names another, as the enterprise build does (PGO_PACKAGE=rasterizeddb_server,
# PGO_BIN=rasterizeddb_core_enterprise). PGO_FEATURES: extra cargo features,
# comma-separated, on top of the package's defaults.
PGO_PACKAGE="${PGO_PACKAGE:-rasterizeddb_core}"
PGO_BIN="${PGO_BIN:-rasterizeddb_core}"
PGO_FEATURES="${PGO_FEATURES:-}"

WORK="$PWD/target/pgo-$TARGET-$PGO_BIN"
BIN="target/$TARGET/release/$PGO_BIN$EXE"
rm -rf "$WORK"
mkdir -p "$WORK/profiles"

HOST="$(rustc -vV | sed -n 's/^host: //p')"
SYSROOT="$(rustc --print sysroot)"
if command -v cygpath >/dev/null 2>&1; then SYSROOT="$(cygpath -u "$SYSROOT")"; fi
PROFDATA="$SYSROOT/lib/rustlib/$HOST/bin/llvm-profdata$EXE"
if [ ! -f "$PROFDATA" ]; then
  echo "::error::llvm-profdata not found at $PROFDATA; run: rustup component add llvm-tools"
  exit 1
fi

build_server() {
  cargo build --locked --release --target "$TARGET" -p "$PGO_PACKAGE" --bin "$PGO_BIN" \
    ${PGO_FEATURES:+--features "$PGO_FEATURES"} \
    --config "target.$TARGET.rustflags=[$1]"
}

echo "::group::1/4 training driver"
cargo build --locked --release --target "$TARGET" -p pgo_train
cp "target/$TARGET/release/pgo_train$EXE" "$WORK/pgo_train$EXE"
TRAIN="$WORK/pgo_train$EXE"
echo "::endgroup::"

echo "::group::2/4 instrumented server and training run"
flags="\"-Cprofile-generate=$(native_path "$WORK/profiles")\""
# On Windows ARM64, Microsoft's link.exe pads the profile names section and
# llvm-profdata then rejects every profile ("symbol name is empty"; LLVM issue
# 243922, rust-lang/rust#150123). The instrumented build links with rust-lld
# instead. The profile matches functions by name and IR hash, not by layout,
# so the optimised build in stage 3 still links with link.exe.
if [ "$TARGET" = "aarch64-pc-windows-msvc" ]; then flags="$flags,\"-Clinker=rust-lld\""; fi
build_server "$flags"
"$TRAIN" "$BIN"
count="$(find "$WORK/profiles" -name '*.profraw' | wc -l)"
if [ "$count" -eq 0 ]; then
  echo "::error::the training run wrote no profiles: the server did not exit cleanly"
  exit 1
fi
"$PROFDATA" merge -o "$WORK/merged.profdata" "$WORK/profiles"
echo "merged $count profile(s)"
"$PROFDATA" show "$WORK/merged.profdata" | tail -n 4
echo "::endgroup::"

echo "::group::3/4 optimised server"
flags="\"-Cprofile-use=$(native_path "$WORK/merged.profdata")\""
# BOLT needs the relocations kept to move functions around.
if [ "$BOLT" = 1 ]; then flags="$flags,\"-Clink-arg=-Wl,--emit-relocs\""; fi
build_server "$flags"
echo "::endgroup::"

if [ "$BOLT" = 1 ]; then
  echo "::group::4/4 BOLT"
  LLVM_BOLT="${LLVM_BOLT:-$(command -v llvm-bolt || command -v llvm-bolt-20 || true)}"
  if [ -z "$LLVM_BOLT" ]; then
    echo "::error::llvm-bolt not found; install bolt-20 or pass --no-bolt"
    exit 1
  fi
  "$LLVM_BOLT" "$BIN" -instrument -o "$WORK/server.instrumented" \
    --instrumentation-file="$WORK/bolt.fdata"
  "$TRAIN" "$WORK/server.instrumented"
  if [ ! -s "$WORK/bolt.fdata" ]; then
    echo "::error::the BOLT training run wrote no profile"
    exit 1
  fi
  "$LLVM_BOLT" "$BIN" -o "$WORK/server.bolt" -data="$WORK/bolt.fdata" \
    -reorder-blocks=ext-tsp -reorder-functions=cdsort -split-functions \
    -split-all-cold -split-eh -dyno-stats
  cp "$WORK/server.bolt" "$BIN"
  echo "::endgroup::"
else
  echo "4/4 BOLT: skipped for $TARGET"
fi

ls -l "$BIN"
