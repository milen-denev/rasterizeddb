# Docker images

Each server release (`v<version>`) pushes images to `ghcr.io/milen-denev/rasterizeddb` for `linux/amd64` and `linux/arm64`. They hold the same binaries as the GitHub release, so nothing is compiled during the image build.

| Tag | Base | Server binary |
|---|---|---|
| `<version>`, `<major>.<minor>`, `<major>`, `latest`, `<version>-ubuntu24.04`, `ubuntu24.04` | Ubuntu 24.04 | glibc, PGO + BOLT |
| `<version>-ubuntu26.04`, `<major>.<minor>-ubuntu26.04`, `ubuntu26.04` | Ubuntu 26.04 | glibc, PGO + BOLT |
| `<version>-alpine`, `<major>.<minor>-alpine`, `alpine` | Alpine 3 | static musl, PGO, mimalloc |

A pre-release (`v1.0.1-rc.1`) gets only `1.0.1-rc.1` and `1.0.1-rc.1-<variant>`; `latest` and the short tags stay on the last full release.

The amd64 images need an x86-64-v3 (AVX2) CPU, like the release binary.

## Running

```sh
docker run -d --name rasterizeddb \
  -e RASTERIZEDDB_PG_PASSWORD=change-me \
  -p 5432:5432 -p 61170:61170 \
  -v rasterizeddb:/var/lib/rasterizeddb \
  ghcr.io/milen-denev/rasterizeddb:latest

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
