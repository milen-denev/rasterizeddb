#!/usr/bin/env bash
# Smoke test of a server binary: start it on a fresh directory, write and read
# through the native protocol with the client's quickstart example, kill it
# hard, start it again and check that what was written is still there.
#
# usage: smoke.sh <path to rasterizeddb_core[.exe]> [quickstart binary]
# Without a quickstart binary it is built for the host with cargo.
set -euo pipefail

BIN="$1"
QUICKSTART="${2:-}"
PORT="${SMOKE_PORT:-61999}"
WORK="$(mktemp -d)"
DB="$WORK/db"
LOG="$WORK/server.log"
SERVER=""

native_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else printf '%s' "$1"; fi
}

cleanup() {
  if [ -n "$SERVER" ]; then kill -9 "$SERVER" 2>/dev/null || true; fi
  rm -rf "$WORK" 2>/dev/null || true
}
trap cleanup EXIT

if [ -z "$QUICKSTART" ]; then
  cargo build --quiet -p rasterizeddb_client --example quickstart
  QUICKSTART="target/debug/examples/quickstart"
  [ -f "$QUICKSTART.exe" ] && QUICKSTART="$QUICKSTART.exe"
fi

start_server() {
  : > "$LOG"
  "$BIN" --location "$(native_path "$DB")" --rdb-port "$PORT" --log-level info >"$LOG" 2>&1 &
  SERVER=$!
  for _ in $(seq 1 120); do
    if grep -q "Database started, listening on" "$LOG"; then return 0; fi
    if ! kill -0 "$SERVER" 2>/dev/null; then
      echo "::error::the server exited during start-up"; cat "$LOG"; exit 1
    fi
    sleep 0.5
  done
  echo "::error::the server did not start within 60 s"; cat "$LOG"; exit 1
}

quickstart() {
  RDB_ADDR="127.0.0.1:$PORT" "$QUICKSTART"
}

echo "== first start"
"$BIN" --version || true
start_server
first="$(quickstart)"
echo "$first"

echo "== hard kill, then restart (WAL recovery)"
kill -9 "$SERVER"
wait "$SERVER" 2>/dev/null || true
SERVER=""
start_server
second="$(quickstart)"
echo "$second"

rows="$(printf '%s\n' "$second" | grep -c "^[0-9][0-9]*: it's a note" || true)"
if [ "$rows" -ne 2 ]; then
  echo "::error::expected the row from before the kill and the new one, found $rows"
  cat "$LOG"
  exit 1
fi

# A graceful stop drains, seals and checkpoints; SIGTERM has no Windows
# equivalent from a shell, so it is checked on unix only.
if [ "$(uname -s)" = "Linux" ] || [ "$(uname -s)" = "Darwin" ]; then
  echo "== graceful stop"
  kill -TERM "$SERVER"
  status=0
  for _ in $(seq 1 60); do
    if ! kill -0 "$SERVER" 2>/dev/null; then break; fi
    sleep 0.5
  done
  wait "$SERVER" || status=$?
  SERVER=""
  if [ "$status" -ne 0 ]; then
    echo "::error::the server exited with $status on SIGTERM"; cat "$LOG"; exit 1
  fi
fi

echo "smoke test passed"
