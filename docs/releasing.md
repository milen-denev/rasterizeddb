# Releasing

There are three release channels. Each is started by pushing a tag, and each makes its own GitHub release.

| Channel | Tag | Workflow | Publishes |
|---|---|---|---|
| Client SDK | `client-v<version>` | `release-client.yml` | crates.io: `rastcp`, `rasterizeddb_client_derive`, `rasterizeddb_client` |
| Server crates | `server-v<version>` | `release-server.yml` | crates.io: `rastcp`, `rasterizeddb_core`, `rasterizeddb_embedded` |
| Server executables | `v<version>` | `release-binaries.yml` | GitHub release assets for 4 targets, plus `SHA256SUMS` |

`<version>` must equal the version in the crates' `Cargo.toml`. For the client channel that is `rasterizeddb_client` and `rasterizeddb_client_derive`; for the server crates and the executables it is `rasterizeddb_core` (and `rasterizeddb_embedded`). The workflow checks this before it builds anything.

## Targets

The executables and the CI matrix cover the same four targets, each built and tested on a native GitHub runner (no cross-compiling):

| Target | Runner | CPU baseline |
|---|---|---|
| `x86_64-unknown-linux-gnu` | `ubuntu-24.04` | x86-64-v3 (AVX2) |
| `aarch64-unknown-linux-gnu` | `ubuntu-24.04-arm` | armv8-a |
| `x86_64-pc-windows-msvc` | `windows-2025` | x86-64-v3 (AVX2) |
| `aarch64-pc-windows-msvc` | `windows-11-arm` | armv8-a |

The per-target compiler flags live in `.cargo/config.toml`: a 32 MiB main-thread stack everywhere, and `target-cpu=x86-64-v3` on x86-64. The workflows never set `RUSTFLAGS`, because doing so would replace those flags instead of adding to them.

## Profile-guided optimisation

Every release executable is built by `.github/scripts/pgo-build.sh`:

1. The server is built instrumented and driven by `pgo_train`, a workspace crate that is never published. It starts the server with both protocols on a fresh directory, runs a mixed workload, checks the row counts, and stops the server gracefully, which is when the profile is written. The workload covers bulk loads, point reads and writes, scans, sorts, aggregates, joins, subqueries, JSONB, upserts, transactions and concurrent clients, over the native protocol and over pgwire, both simple and prepared.
2. The server is built again from the merged profile.
3. On Linux only, `llvm-bolt` instruments that binary, the same workload runs again, and BOLT rewrites the binary's code layout from what it recorded. BOLT handles only ELF, so the Windows binaries stop after step 2.

The flags reach rustc through `cargo --config target.<triple>.rustflags=[…]`, which cargo appends to the flags in `.cargo/config.toml`.

A graceful stop is SIGTERM on Linux and Ctrl+Break on Windows. `pgo_train` starts the server in its own process group, so the Ctrl+Break reaches the server alone.

To build a release binary locally the same way, run `rustup component add llvm-tools`, then `bash .github/scripts/pgo-build.sh x86_64-unknown-linux-gnu`. Add `--no-bolt` if `llvm-bolt` is not installed. A manual run of "Release - server executables" can switch PGO off with its `pgo` input. A pull request that changes the build files runs the whole build as a check, without making a release.

## Order

1. Bump the versions in the `Cargo.toml` files and merge.
2. `client-v<x>`: wait for it to finish. The embedded SDK depends on the client, and `release-server.yml` refuses to run until that client version is on crates.io.
3. `server-v<x>`.
4. `v<x>`.

`rastcp` is published by whichever crate channel first finds its version missing from crates.io. Every publish step skips versions that already exist, so a run that failed partway can be re-run as it is.

## Before the first release

- Add the repository secret `CARGO_REGISTRY_TOKEN`: a crates.io API token with the `publish-new` and `publish-update` scopes. Once the crates exist you can switch to crates.io trusted publishing (`rust-lang/crates-io-auth-action`) and delete the token.
- The tag workflows need `contents: write` (already declared) to create releases.
- Dry run: Actions -> "Release - client crates" or "Release - server crates" -> Run workflow, with `dry_run` checked. It packages and verifies every crate the way crates.io would build it, and uploads nothing.

## What each release checks first

- **Client:** the client, derive and rastcp tests on all four targets, plus on Linux the live tests against this tree's server binary.
- **Server crates:** the embedded SDK tests and a server smoke test (start, write, `kill -9`, restart, read back) on all four targets.
- **Executables:** the same smoke test, run on the release binary of each target before it is packaged.

CI (`ci.yml`) runs the four-target build and tests on every push and pull request. The engine's long suites (core lib, core integration tests, compat) run on all four targets weekly, on demand (`full = true`) and on pushes to `feat/secondary-indexes`.
