#!/usr/bin/env bash
# Smoke-tests the RasterizedDB + RustFS image (docker/Dockerfile.rustfs):
#   - it refuses to start without its passwords, and with RustFS's default;
#   - with them, a table placed on the S3 store lives in RustFS, a file table
#     beside it does not, and a BACKUP DATABASE lands in the backup bucket
#     with no snapshot failure logged;
#   - `docker stop` exits cleanly and both tables survive a restart.
#
# usage: docker-smoke-rustfs.sh <image> [host port]     (needs docker and psql)
set -euo pipefail

IMAGE="${1:?usage: docker-smoke-rustfs.sh <image> [host port]}"
PORT="${2:-55433}"
NAME="rdb-rustfs-smoke-$$"
PASSWORD="smoke-$$-pw"
KEY="smoke$$key"
SECRET="smoke-$$-secret-key"
CONN="host=127.0.0.1 port=$PORT user=rasterized dbname=postgres sslmode=require connect_timeout=2"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker volume rm -f "$NAME-data" "$NAME-db" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sql() { PGPASSWORD="$PASSWORD" psql "$CONN" -qAtX -v ON_ERROR_STOP=1 "$@"; }
rc_in() { docker exec -e RC_CONFIG_DIR=/tmp -e "RC_HOST_s=http://$KEY:$SECRET@127.0.0.1:9000" "$NAME" rc "$@"; }

wait_ready() { # wait_ready <how many "ready" lines>
  for _ in $(seq 1 180); do
    [ "$(docker logs "$NAME" 2>&1 | grep -c 'rasterizeddb-rustfs: ready')" -ge "$1" ] && return 0
    if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != true ]; then
      echo "::error::the container exited"; docker logs "$NAME" 2>&1 | tail -30; return 1
    fi
    sleep 1
  done
  echo "::error::not ready in 180 s"; docker logs "$NAME" 2>&1 | tail -30
  return 1
}

refuses() { # refuses <expected text> <docker run args...>
  local want="$1" out; shift
  if out="$(timeout 60 docker run --rm "$@" "$IMAGE" 2>&1)"; then
    echo "::error::the container started ($want expected)"; echo "$out" | tail -5; exit 1
  fi
  echo "$out" | grep -q "$want" || { echo "::error::expected '$want'"; echo "$out" | tail -5; exit 1; }
}

echo "--- refuses without passwords, and with RustFS's default"
refuses "set RASTERIZEDDB_PG_PASSWORD"
refuses "set RUSTFS_ACCESS_KEY" -e RASTERIZEDDB_PG_PASSWORD=x
refuses "must not be RustFS's default" -e RASTERIZEDDB_PG_PASSWORD=x -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin

echo "--- start"
docker run -d --name "$NAME" -e RASTERIZEDDB_PG_PASSWORD="$PASSWORD" \
  -e RUSTFS_ACCESS_KEY="$KEY" -e RUSTFS_SECRET_KEY="$SECRET" \
  -p "127.0.0.1:$PORT:5432" -v "$NAME-data:/data" -v "$NAME-db:/var/lib/rasterizeddb" "$IMAGE" >/dev/null
wait_ready 1

echo "--- an S3 table and a file table"
sql -c "STORAGE PLACE TABLE on_s3 AT 'kv:rustfs'" \
    -c 'CREATE TABLE on_s3 (id INT PRIMARY KEY, v TEXT)' \
    -c "INSERT INTO on_s3 VALUES (1, 'one'), (2, 'two')" \
    -c 'CREATE TABLE on_disk (id INT PRIMARY KEY, v TEXT)' \
    -c "INSERT INTO on_disk VALUES (1, 'disk')"
backend() { sql -c 'SHOW TABLES' | awk -F'|' -v t="$1" '$1 == t {print $2}'; }
[ "$(backend on_s3)" = s3 ] || { echo "::error::on_s3 is on '$(backend on_s3)', not s3"; exit 1; }
[ "$(backend on_disk)" = file ] || { echo "::error::on_disk is on '$(backend on_disk)', not file"; exit 1; }
rc_in object list --recursive s/rasterizeddb-tables | grep -q 'on_s3' \
  || { echo "::error::no on_s3 object in the tables bucket"; exit 1; }

echo "--- a backup lands in the backup bucket"
sql -c 'BACKUP DATABASE' >/dev/null
sql -c 'SHOW BACKUPS' | grep -q '^snapshot|' || { echo "::error::SHOW BACKUPS lists no snapshot"; exit 1; }
rc_in object list --recursive s/rasterizeddb-backup | grep -q 'snapshots/' \
  || { echo "::error::no snapshot object in the backup bucket"; exit 1; }
if docker logs "$NAME" 2>&1 | grep -q 'snapshot failed'; then
  echo "::error::a snapshot failed"; docker logs "$NAME" 2>&1 | grep 'snapshot failed' | tail -3; exit 1
fi

echo "--- docker stop exits cleanly"
docker stop -t 90 "$NAME" >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' "$NAME")"
[ "$code" = 0 ] || { echo "::error::exit code $code after docker stop"; docker logs "$NAME" 2>&1 | tail -20; exit 1; }

echo "--- both tables survive a restart"
docker start "$NAME" >/dev/null
wait_ready 2
[ "$(sql -c 'SELECT v FROM on_s3 ORDER BY id' | paste -sd, -)" = "one,two" ] || { echo "::error::on_s3 after a restart"; exit 1; }
[ "$(sql -c 'SELECT v FROM on_disk' )" = disk ] || { echo "::error::on_disk after a restart"; exit 1; }

echo "image $IMAGE: ok"
