# Docker images

Each server release (`v<version>`) pushes images to Docker Hub as `milendenev/rasterizeddb` and to GitHub's registry as `ghcr.io/milen-denev/rasterizeddb`, for `linux/amd64` and `linux/arm64`, with the same tags in both. They hold the same binaries as the GitHub release, so nothing is compiled during the image build.

| Tag | Base | Server binary |
|---|---|---|
| `<version>`, `<major>.<minor>`, `<major>`, `latest`, `<version>-ubuntu24.04`, `ubuntu24.04` | Ubuntu 24.04 | glibc, PGO + BOLT |
| `<version>-ubuntu26.04`, `<major>.<minor>-ubuntu26.04`, `ubuntu26.04` | Ubuntu 26.04 | glibc, PGO + BOLT |
| `<version>-alpine`, `<major>.<minor>-alpine`, `alpine` | Alpine 3 | static musl, PGO, mimalloc |
| `<version>-rustfs`, `<major>.<minor>-rustfs`, `rustfs` | the official RustFS image (Alpine) | static musl, with RustFS beside it; see [RustFS](#rustfs-backups-and-s3-tables-in-one-container) |

A pre-release (`v1.0.1-rc.1`) gets only `1.0.1-rc.1` and `1.0.1-rc.1-<variant>`; `latest` and the short tags stay on the last full release.

The amd64 images need an x86-64-v3 (AVX2) CPU, like the release binary.

## Running

```sh
docker run -d --name rasterizeddb \
  -e RASTERIZEDDB_PG_PASSWORD=change-me \
  -p 5432:5432 -p 61170:61170 \
  -v rasterizeddb:/var/lib/rasterizeddb \
  milendenev/rasterizeddb:latest

psql "host=localhost user=rasterized dbname=postgres sslmode=require"
```

- **Password.** `RASTERIZEDDB_PG_PASSWORD` is the password of the role `rasterized`, which the server creates when its role catalogue is empty, so it matters on the first start. The PostgreSQL protocol and the native protocol both log in with it. Without a password the server refuses to listen on the container's network, and the container exits with that error.
- **Ports.** 5432 is the PostgreSQL protocol and 61170 the native protocol (`rasterizeddb_client`, RasterizedDB Studio).
- **TLS.** The PostgreSQL protocol requires TLS by default, using a self-signed certificate made at start-up. Clients connect with `sslmode=require`. To give the server your own certificate, mount it and add `--pg-cert /certs/server.crt --pg-key /certs/server.key`. To also accept cleartext connections, add `--pg-tls prefer`.
- **Data.** Data lives in `/var/lib/rasterizeddb`, which is a volume. The server runs as uid/gid 10001 in every variant, so a volume can move between them. A bind-mounted host directory must be writable by uid 10001.
- **Stopping.** `docker stop` sends SIGTERM, which shuts the server down cleanly. Give it time to finish (`docker stop -t 60`, or `stop_grace_period: 60s` in compose).

## Server flags

The entrypoint is

```
rasterizeddb --location /var/lib/rasterizeddb --protocol both --pg-addr 0.0.0.0 --rdb-addr 0.0.0.0
```

and anything after the image name is appended to it, for example:

```sh
docker run ... ghcr.io/milen-denev/rasterizeddb:latest --max-connections 200 --log-level warn
docker run ... ghcr.io/milen-denev/rasterizeddb:latest --mysql-port 3306 --mysql-addr 0.0.0.0
```

`docker run --rm ghcr.io/milen-denev/rasterizeddb:latest --help` lists every flag. To change one of the entrypoint's own flags, replace the entrypoint with `--entrypoint rasterizeddb` and give the full command.

## Compose

```yaml
services:
  rasterizeddb:
    image: ghcr.io/milen-denev/rasterizeddb:latest
    environment:
      RASTERIZEDDB_PG_PASSWORD: ${RASTERIZEDDB_PG_PASSWORD:?set it}
    ports: ["5432:5432", "61170:61170"]
    volumes: ["rasterizeddb:/var/lib/rasterizeddb"]
    stop_grace_period: 60s
    restart: unless-stopped
volumes:
  rasterizeddb:
```

## RustFS: backups and S3 tables in one container

The `rustfs` image is built on the official [RustFS](https://github.com/rustfs/rustfs) image (S3-compatible object storage, Apache-2.0) and runs the server beside it:

```sh
docker run -d --name rasterizeddb \
  -e RASTERIZEDDB_PG_PASSWORD=change-me \
  -e RUSTFS_ACCESS_KEY=rdb-admin -e RUSTFS_SECRET_KEY=change-me-too \
  -p 5432:5432 -p 61170:61170 -p 9000:9000 -p 9001:9001 \
  -v rasterizeddb-db:/var/lib/rasterizeddb -v rasterizeddb-s3:/data \
  milendenev/rasterizeddb:rustfs
```

At start-up, RustFS starts first and two buckets are created if they are missing. Then the server starts with:

- **Continuous backup** to the bucket `rasterizeddb-backup`: snapshots plus the WAL archive. `BACKUP DATABASE`, `SHOW BACKUPS` and a restore (`--restore-from-s3`, [s3-backup.md](s3-backup.md)) all use it.
- **S3-backed tables** in the bucket `rasterizeddb-tables`, through a store named `rustfs`. Placing a table there before creating it keeps its rows in RustFS rather than in `/var/lib/rasterizeddb`:

  ```sql
  STORAGE PLACE TABLE events AT 'kv:rustfs';
  CREATE TABLE events (id BIGINT PRIMARY KEY, body TEXT);
  STORAGE MOVE TABLE old_events TO 'kv:rustfs';   -- convert an existing table
  ```

  A backup copies such a table as of each snapshot, with no point-in-time replay ([kv-storage.md](kv-storage.md)).

| Setting | Default | |
|---|---|---|
| `RASTERIZEDDB_PG_PASSWORD` | required | the role `rasterized`, both protocols |
| `RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY` | required, not `rustfsadmin` | RustFS's root credentials; the server signs with them too |
| `RASTERIZEDDB_BACKUP_BUCKET` | `rasterizeddb-backup` | |
| `RASTERIZEDDB_TABLES_BUCKET` | `rasterizeddb-tables` | |
| `RASTERIZEDDB_S3_STORE` | `rustfs` | the store name in `kv:<name>` |
| `RASTERIZEDDB_S3_BACKUP` | `on` | `off` leaves backups off |

- **Ports.** 9000 is the S3 API and 9001 the RustFS console. Publish them only if something outside the container needs them; the server reaches RustFS inside it.
- **Volumes.** `/var/lib/rasterizeddb` holds the database and `/data` holds RustFS's objects. A backup on the same disk as the database survives a dropped table or a bad deploy, not a lost disk, so put `/data` on another disk, or copy the bucket elsewhere (`rc mirror` ships in the image).
- **Stopping.** `docker stop` stops the server first, which still writes to RustFS while it shuts down, and then RustFS. If either process exits on its own, the other is stopped too, so a restart policy brings both back.
- **Process.** Both run as RustFS's user (uid 10001), and the server's credentials come from the environment, never its command line. Arguments after the image name go to the server.

## Building an image yourself

The Dockerfiles in `docker/` take the server binary from the build context:

```sh
mkdir -p docker/bin/amd64
cp target/release/rasterizeddb_core docker/bin/amd64/rasterizeddb   # glibc build
cp README.md LICENSE-GPL docker/
docker build --platform linux/amd64 -f docker/Dockerfile.ubuntu -t rasterizeddb docker
bash .github/scripts/docker-smoke.sh rasterizeddb                     # needs psql
```

A binary built on a newer distribution than the image needs a newer glibc than the image has, so build it on Ubuntu 24.04, or take it from a release. `Dockerfile.alpine` reads `bin-musl/<arch>/rasterizeddb`, the `x86_64-unknown-linux-musl` or `aarch64-unknown-linux-musl` build. That build needs musl's C compiler (`musl-tools`) and `CC_<target>=musl-gcc`.
