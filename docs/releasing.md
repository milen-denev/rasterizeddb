# Releasing

There are three release channels. Each is started by pushing a tag, and each makes its own GitHub release.

| Channel | Tag | Workflow | Publishes |
|---|---|---|---|
| Client SDK | `client-v<version>` | `release-client.yml` | crates.io: `rastcp`, `rasterizeddb_client_derive`, `rasterizeddb_client` |
| Server crates | `server-v<version>` | `release-server.yml` | crates.io: `rastcp`, `rasterizeddb_core`, `rasterizeddb_embedded` |
| Server executables | `v<version>` | `release-binaries.yml` | GitHub release assets: the server for 6 targets (4 glibc/MSVC + 2 static musl), Studio for 4, the Arch/CachyOS package and its AUR sources, `SHA256SUMS`; container images on `ghcr.io` (Ubuntu 24.04, Ubuntu 26.04, Alpine) |

`<version>` must equal the version in the crates' `Cargo.toml`. For the client channel that is `rasterizeddb_client` and `rasterizeddb_client_derive`; for the server crates and the executables it is `rasterizeddb_core`. `rasterizeddb_embedded` has its own version, published by the same `server-v` release. The workflow checks this before it builds anything.

The executables channel also takes a pre-release tag, such as `v1.0.0-rc.1`. The suffix is set aside for the version check, and the GitHub release is marked as a pre-release, so "latest" still points at the last full release. The crate channels take no suffix, because crates.io would receive the version in `Cargo.toml`, not the tag's.

## One command: `push_release.sh`

`./push_release.sh` at the repository root runs a whole release:

```sh
./push_release.sh client minor        # client crates 1.0.0 -> 1.1.0, tag client-v1.1.0
./push_release.sh server patch        # server crates and executables, tags server-vX.Y.Z and vX.Y.Z
./push_release.sh all patch --embedded minor   # everything a patch, the embedded SDK a minor
./push_release.sh all current         # first release: both channels at the version in Cargo.toml
./push_release.sh binaries --rc       # executables only, as a pre-release v<core>-rc.N
./push_release.sh client minor --dry-run   # show the version commit, change nothing
```

Branches: work lands on `dev`; `release` holds exactly what was released; `main` (the GitHub default) follows `release` by fast-forward only. No other branch lives on the remote, apart from the script's short-lived `release-pr/<tag>`.

It releases what is on `origin/dev` (`--from` for another branch), working in a scratch worktree so the local checkout is left alone:

1. It bumps the channel's crate versions, every `{ path, version }` requirement on them, and README pins such as `rasterizeddb_embedded = "1"`, then refreshes `Cargo.lock`.
2. It commits on `release-pr/<tag>` and opens a pull request into `release`.
3. It waits for the pull request's own runs: the `pull_request` runs of that branch at that commit. It does not wait for every check on the commit, because a release with no version change has the source tip as its head, and that tip also carries the push runs and their full suites, which a pull request never runs. If a run fails, it stops and leaves the pull request open.
4. It merges with a merge commit, and only if the head is still the commit that was tested. Then it fast-forwards `main` to that merge commit: its checks already ran on exactly that commit, so `main` gets no second pull request and no second CI run. If `main` ever has a commit `release` lacks, the script stops there, before any tag.
5. It brings the version commit back to the source branch: a fast-forward when that branch has not moved, a merge commit otherwise.
6. It tags the merge commit and pushes the tags. `all` waits for the client release first, because the server crates need it on crates.io. Then it waits for the release workflows and prints the release links.

Each channel has its own version: the client channel owns `rasterizeddb_client` and `rasterizeddb_client_derive`, and the server channel owns `rasterizeddb_core` and `rasterizeddb_embedded`. The server tags and the executables carry the core version. The embedded SDK keeps its own version, so it can take a minor bump for new API while the engine and executables take a patch: `--embedded major|minor|patch|current|X.Y.Z`. Without it the SDK gets the same bump as the server, applied to its own version.

`rastcp` is shared. When its code changed since the last crate release and its current version is already on crates.io, the script bumps it a patch; `--rastcp keep|minor|major|X.Y.Z` overrides that.

To rerun after a stop, run the same command again. The script moves the `release/<tag>` branch it left to the new head and reuses the pull request that is still open from it. When the head has not changed (a flaky check), no new run starts, so the script re-runs the failed jobs of the last attempt once before it waits. Nothing is tagged before the merge, so a stopped run leaves only that branch and pull request.

A version that is already tagged or on crates.io is always refused, and so is a server release whose client version is not on crates.io yet.

It needs `git`, `gh` (logged in), `cargo`, `python3` and `curl`.

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

CI (`ci.yml`) runs the four-target build and tests on every push to `dev` and every pull request. The engine's long suites (core lib, core integration tests, compat) run on all four targets weekly, on demand (`full = true`) and on pushes to `dev`.
