#!/usr/bin/env bash
# Smoke-tests a RasterizedDB server image: without a password it must refuse
# to start; with one it serves the PostgreSQL protocol over TLS, stops cleanly
# on `docker stop`, keeps its rows across a restart on a volume, and runs as
# its own non-root user.
#
# usage: docker-smoke.sh <image> [host port]     (needs docker and psql)
set -euo pipefail

IMAGE="${1:?usage: docker-smoke.sh <image> [host port]}"
PORT="${2:-55432}"
NAME="rdb-smoke-$$"
VOLUME="rdb-smoke-$$"
PASSWORD="smoke-$$-pw"
CONN="host=127.0.0.1 port=$PORT user=rasterized dbname=postgres sslmode=require connect_timeout=2"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker volume rm -f "$VOLUME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sql() { PGPASSWORD="$PASSWORD" psql "$CONN" -qAtX -v ON_ERROR_STOP=1 "$@"; }

wait_ready() {
  for _ in $(seq 1 60); do
    if sql -c 'SELECT 1' >/dev/null 2>&1; then return 0; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != true ]; then
      echo "::error::the container exited"; docker logs "$NAME" 2>&1 | tail -20; return 1
    fi
    sleep 1
  done
  echo "::error::the server did not accept a connection in 60 s"; docker logs "$NAME" 2>&1 | tail -20
  return 1
}

echo "--- without a password the container refuses to start"
if out="$(timeout 60 docker run --rm "$IMAGE" 2>&1)"; then
  echo "::error::the container started without a password"; echo "$out" | tail -5; exit 1
fi
echo "$out" | grep -q "refusing to serve" || { echo "::error::unexpected exit"; echo "$out" | tail -5; exit 1; }

echo "--- the version"
docker run --rm "$IMAGE" --version

echo "--- start with a password and a volume"
docker volume create "$VOLUME" >/dev/null
docker run -d --name "$NAME" -e RASTERIZEDDB_PG_PASSWORD="$PASSWORD" \
  -p "127.0.0.1:$PORT:5432" -v "$VOLUME:/var/lib/rasterizeddb" "$IMAGE" >/dev/null
wait_ready
sql -c 'CREATE TABLE smoke (id INT PRIMARY KEY, v TEXT)' \
    -c "INSERT INTO smoke VALUES (1, 'one'), (2, 'two')"

uid="$(docker exec "$NAME" id -u)"
[ "$uid" = 10001 ] || { echo "::error::the server runs as uid $uid, not 10001"; exit 1; }

echo "--- docker stop exits cleanly"
docker stop -t 60 "$NAME" >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' "$NAME")"
[ "$code" = 0 ] || { echo "::error::exit code $code after docker stop"; docker logs "$NAME" 2>&1 | tail -20; exit 1; }

echo "--- the rows survive a restart"
docker start "$NAME" >/dev/null
wait_ready
got="$(sql -c 'SELECT v FROM smoke ORDER BY id' | paste -sd, -)"
[ "$got" = "one,two" ] || { echo "::error::read back '$got' after a restart"; exit 1; }

echo "image $IMAGE: ok"
