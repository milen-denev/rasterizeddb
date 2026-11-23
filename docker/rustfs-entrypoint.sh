#!/bin/sh
# RasterizedDB and RustFS in one container (docker/Dockerfile.rustfs).
#
# RustFS (S3-compatible object storage, http://127.0.0.1:9000 inside the
# container) starts first, through its own entrypoint and its own checks. Two
# buckets are made if missing; then the server starts with:
#
#   - continuous backup (snapshots + WAL archive) to the backup bucket, which
#     BACKUP DATABASE / SHOW BACKUPS / --restore-from-s3 use;
#   - a store for S3-backed tables over the tables bucket, so that
#       STORAGE PLACE TABLE t AT 'kv:rustfs';  CREATE TABLE t (...);
#     keeps t in RustFS rather than in /var/lib/rasterizeddb.
#
# A stop (SIGTERM) stops the database first, which still writes to RustFS
# while it shuts down, and RustFS after it. If either process ends on its own,
# the other is stopped and the container exits non-zero, so a restart policy
# brings back both.
#
# Arguments are appended to the server's command line.
set -eu

fail() { echo "rasterizeddb-rustfs: $*" >&2; exit 1; }

[ -n "${RASTERIZEDDB_PG_PASSWORD:-}" ] \
  || fail "set RASTERIZEDDB_PG_PASSWORD: the password of the role rasterized, for both protocols"
[ -n "${RUSTFS_ACCESS_KEY:-}" ] && [ -n "${RUSTFS_SECRET_KEY:-}" ] \
  || fail "set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY: RustFS's root credentials, which the database signs its backups and S3 tables with"
[ "$RUSTFS_ACCESS_KEY" != rustfsadmin ] && [ "$RUSTFS_SECRET_KEY" != rustfsadmin ] \
  || fail "RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY must not be RustFS's default (rustfsadmin)"

BACKUP_BUCKET="${RASTERIZEDDB_BACKUP_BUCKET:-rasterizeddb-backup}"
TABLES_BUCKET="${RASTERIZEDDB_TABLES_BUCKET:-rasterizeddb-tables}"
STORE="${RASTERIZEDDB_S3_STORE:-rustfs}"
BACKUP="${RASTERIZEDDB_S3_BACKUP:-on}"
S3="http://127.0.0.1:9000"
DATA="/var/lib/rasterizeddb"

# Names go into SQL and S3 paths: S3's bucket rules, and a plain identifier.
for b in "$BACKUP_BUCKET" "$TABLES_BUCKET"; do
  echo "$b" | grep -Eq '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$' || fail "'$b' is not a valid S3 bucket name"
done
[ "$BACKUP_BUCKET" != "$TABLES_BUCKET" ] || fail "the backup and tables buckets must differ"
echo "$STORE" | grep -Eq '^[a-z_][a-z0-9_]{0,62}$' || fail "'$STORE' is not a valid store name (lower-case letters, digits, _)"
case "$BACKUP" in on|off) ;; *) fail "RASTERIZEDDB_S3_BACKUP is on or off, not '$BACKUP'" ;; esac

RUSTFS_PID=""
DB_PID=""
RC_CONFIG_DIR=""

stop_all() {
  trap - TERM INT HUP
  if [ -n "$DB_PID" ] && kill -0 "$DB_PID" 2>/dev/null; then
    kill -TERM "$DB_PID" 2>/dev/null || true
    wait "$DB_PID" 2>/dev/null || true
  fi
  if [ -n "$RUSTFS_PID" ] && kill -0 "$RUSTFS_PID" 2>/dev/null; then
    kill -TERM "$RUSTFS_PID" 2>/dev/null || true
    wait "$RUSTFS_PID" 2>/dev/null || true
  fi
  [ -z "$RC_CONFIG_DIR" ] || rm -rf "$RC_CONFIG_DIR"
}
trap 'stop_all; exit 0' TERM INT HUP

# ---- RustFS
/etc/rustfs/entrypoint.sh rustfs &
RUSTFS_PID=$!
i=0
until curl -fsS "$S3/health/ready" >/dev/null 2>&1; do
  kill -0 "$RUSTFS_PID" 2>/dev/null || { RUSTFS_PID=""; fail "RustFS exited before it was ready"; }
  i=$((i + 1))
  [ "$i" -lt 240 ] || { stop_all; fail "RustFS was not ready after 120 s"; }
  sleep 0.5
done

# ---- buckets (rc reads the alias, credentials included, from the environment)
uri() { jq -rn --arg v "$1" '$v | @uri'; }
RC_CONFIG_DIR="$(mktemp -d)"
export RC_CONFIG_DIR
RC_HOST_local="http://$(uri "$RUSTFS_ACCESS_KEY"):$(uri "$RUSTFS_SECRET_KEY")@127.0.0.1:9000"
export RC_HOST_local
for b in "$BACKUP_BUCKET" "$TABLES_BUCKET"; do
  rc bucket create "local/$b" --ignore-existing >/dev/null || { stop_all; fail "could not create bucket $b"; }
done
unset RC_HOST_local

# ---- the database: signs with RustFS's root credentials, from the
# environment (never on its command line, which `ps` shows)
export AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY"
export AWS_REGION="${AWS_REGION:-us-east-1}"
if [ "$BACKUP" = on ]; then
  set -- --s3-endpoint "$S3" --s3-bucket "$BACKUP_BUCKET" --s3-path-style "$@"
fi
rasterizeddb --location "$DATA" --protocol both --pg-addr 0.0.0.0 --rdb-addr 0.0.0.0 "$@" &
DB_PID=$!

# ---- the S3 table store, once the server answers
export PGPASSWORD="$RASTERIZEDDB_PG_PASSWORD"
PSQL="host=127.0.0.1 port=5432 user=rasterized dbname=postgres sslmode=require connect_timeout=2"
i=0
until psql "$PSQL" -qAtX -c 'SELECT 1' >/dev/null 2>&1; do
  kill -0 "$DB_PID" 2>/dev/null || { DB_PID=""; stop_all; fail "the database exited during start-up"; }
  i=$((i + 1))
  [ "$i" -lt 600 ] || { stop_all; fail "the database did not answer on port 5432 within 10 minutes"; }
  sleep 1
done
psql "$PSQL" -qAtX -v ON_ERROR_STOP=1 -c "STORAGE ADD STORE IF NOT EXISTS $STORE S3 WITH (ENDPOINT = '$S3', BUCKET = '$TABLES_BUCKET', REGION = '$AWS_REGION', ADDRESSING = 'path')" >/dev/null \
  || { stop_all; fail "could not define the S3 table store $STORE"; }
unset PGPASSWORD
echo "rasterizeddb-rustfs: ready. S3 tables: STORAGE PLACE TABLE <name> AT 'kv:$STORE'; then CREATE TABLE <name>. Backups: bucket $BACKUP_BUCKET ($BACKUP)."

# ---- supervise: whichever ends first takes the other with it
while kill -0 "$DB_PID" 2>/dev/null && kill -0 "$RUSTFS_PID" 2>/dev/null; do
  sleep 1 &
  wait $! || true
done
if ! kill -0 "$DB_PID" 2>/dev/null; then
  wait "$DB_PID" 2>/dev/null && code=0 || code=$?
  DB_PID=""
  echo "rasterizeddb-rustfs: the database exited ($code); stopping RustFS" >&2
else
  wait "$RUSTFS_PID" 2>/dev/null && code=0 || code=$?
  RUSTFS_PID=""
  echo "rasterizeddb-rustfs: RustFS exited ($code); stopping the database" >&2
fi
stop_all
[ "$code" != 0 ] || code=1
exit "$code"
