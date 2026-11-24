#!/usr/bin/env bash
# push_release.sh: cut a release: dev -> pull request into release -> main.
#
# Branches: work lands on `dev`; `release` holds exactly what was released,
# and `main`, the GitHub default, follows `release` by fast-forward only.
# Nothing else lives on the remote except this script's short-lived
# `release-pr/<tag>` branch, deleted once its pull request is merged.
#
#   ./push_release.sh client   <bump>    client crates      -> client-vX.Y.Z
#   ./push_release.sh server   <bump>    server crates      -> server-vX.Y.Z and vX.Y.Z
#   ./push_release.sh all      <bump>    client, then server (both channels)
#   ./push_release.sh binaries [--rc]    executables only   -> v<core version>[-rc.N]
#
# <bump> is major, minor, patch, an explicit X.Y.Z, or `current`: release the
# version already in Cargo.toml (for a first release, such as 1.0.0). A version
# that is already tagged or on crates.io is always refused.
#
# Options:
#   --from <branch>   branch to release from (default: $RELEASE_SOURCE or dev)
#   --embedded <major|minor|patch|current|X.Y.Z>
#                     server channel: the rasterizeddb_embedded version, when the
#                     SDK moves differently from the engine (default: the same
#                     bump as the server, applied to the SDK's own version)
#   --rastcp <keep|major|minor|patch|X.Y.Z>
#                     what to do with the shared rastcp crate (default: bump it a
#                     patch when it changed since the last crate release and its
#                     current version is already on crates.io)
#   --rc              binaries only: a release candidate, published as a GitHub pre-release
#   --dry-run         make the version commit in a scratch worktree, show it, change nothing anywhere
#   --no-wait         stop once the tags are pushed instead of waiting for the release workflows
#   --yes             do not ask for confirmation
#
# What it does, in order:
#   1. Releases what is on origin/<from> (not your local branch), in a scratch
#      git worktree, so your checkout is never touched.
#   2. Bumps the channel's crate versions, the version requirements that point
#      at them, and README pins; refreshes Cargo.lock; commits on a new branch
#      release-pr/<first tag>.
#   3. Opens a pull request from that branch into release and waits for every
#      check. If one fails it stops and leaves the pull request open.
#   4. Merges the pull request (a merge commit, only if the head is still the
#      commit it tested) and fast-forwards main to it: the checks already ran
#      on exactly that commit, so main gets no pull request and no second CI
#      run. Then it brings the version commit back to <from>: a fast-forward
#      when <from> has not moved, a merge commit otherwise.
#   5. Tags the merge commit and pushes the tags in channel order. The
#      server crates need the client version on crates.io, so `all` waits for
#      the client release before pushing the server tags.
#   6. Waits for the release workflows and prints the GitHub release links.
#
# Each channel has its own version. The client channel owns
# rasterizeddb_client and rasterizeddb_client_derive; the server channel owns
# rasterizeddb_core and rasterizeddb_embedded. The server tags (server-vX.Y.Z,
# vX.Y.Z) and the executables carry the core version; the embedded SDK has its
# own (--embedded), which the server release publishes beside the core. rastcp
# is shared: whichever channel publishes first publishes it.
set -euo pipefail

SOURCE="${RELEASE_SOURCE:-dev}"
BASE="release"
# Follows BASE by fast-forward after each release.
PUBLISHED="main"
REMOTE="origin"
CLIENT_CRATES=(rasterizeddb_client rasterizeddb_client_derive)
SERVER_CRATES=(rasterizeddb_core rasterizeddb_embedded)
SHARED_CRATE="rastcp"

# ---------------------------------------------------------------- output

if [ -t 1 ]; then BOLD=$'\e[1m'; RED=$'\e[31m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RESET=$'\e[0m'
else BOLD=""; RED=""; GREEN=""; YELLOW=""; RESET=""; fi
step() { printf '\n%s==> %s%s\n' "$BOLD" "$*" "$RESET"; }
note() { printf '    %s\n' "$*"; }
warn() { printf '%swarning:%s %s\n' "$YELLOW" "$RESET" "$*" >&2; }
die()  { printf '%serror:%s %s\n' "$RED" "$RESET" "$*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit "${1:-0}"; }

# ---------------------------------------------------------------- arguments

CHANNEL="${1:-}"; [ $# -gt 0 ] && shift
case "$CHANNEL" in
  client|server|all|binaries) ;;
  -h|--help|"") usage 0 ;;
  *) die "unknown channel '$CHANNEL' (client, server, all or binaries)" ;;
esac
BUMP=""
if [ "$CHANNEL" != binaries ]; then
  BUMP="${1:-}"; [ $# -gt 0 ] && shift
  [ -n "$BUMP" ] || die "$CHANNEL needs a bump: major, minor, patch, current or an explicit X.Y.Z"
fi
RASTCP_BUMP="auto"; EMBEDDED_BUMP=""; RC=0; DRY_RUN=0; WAIT=1; YES=0
while [ $# -gt 0 ]; do
  case "$1" in
    --from) SOURCE="${2:?--from needs a branch}"; shift 2 ;;
    --embedded) EMBEDDED_BUMP="${2:?--embedded needs major, minor, patch, current or X.Y.Z}"; shift 2 ;;
    --rastcp) RASTCP_BUMP="${2:?--rastcp needs keep, major, minor, patch or X.Y.Z}"; shift 2 ;;
    --rc) RC=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --no-wait) WAIT=0; shift ;;
    --yes|-y) YES=1; shift ;;
    -h|--help) usage 0 ;;
    *) die "unknown option '$1'" ;;
  esac
done
[ -z "$EMBEDDED_BUMP" ] || [ "$CHANNEL" = server ] || [ "$CHANNEL" = all ] || die "--embedded is for the server and all channels"
[ "$RC" = 0 ] || [ "$CHANNEL" = binaries ] || die "--rc is for the binaries channel only: crates.io gets exactly the Cargo.toml version"

# ---------------------------------------------------------------- helpers

for tool in git gh cargo python3 curl; do
  command -v "$tool" >/dev/null || die "$tool is not installed"
done
REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null)" || die "run this inside the repository"
cd "$REPO_DIR"

# Semver arithmetic: next_version <current> <major|minor|patch|X.Y.Z>
next_version() {
  python3 - "$1" "$2" <<'PY'
import re, sys
cur, bump = sys.argv[1], sys.argv[2]
m = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", cur)
if not m:
    sys.exit(f"current version {cur} is not X.Y.Z")
major, minor, patch = map(int, m.groups())
if bump == "current": new = (major, minor, patch)
elif bump == "major":   new = (major + 1, 0, 0)
elif bump == "minor": new = (major, minor + 1, 0)
elif bump == "patch": new = (major, minor, patch + 1)
else:
    b = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", bump)
    if not b:
        sys.exit(f"'{bump}' is not major, minor, patch or X.Y.Z")
    new = tuple(map(int, b.groups()))
    if new < (major, minor, patch) or (new == (major, minor, patch) and bump != cur):
        sys.exit(f"{bump} is not newer than the current {cur}")
print("%d.%d.%d" % new)
PY
}

# The [package] version of a crate on a commit.
version_at() {
  git show "$1:$2/Cargo.toml" | python3 -c '
import sys, tomllib
print(tomllib.loads(sys.stdin.read())["package"]["version"])'
}

# 0 when that exact version is on crates.io, 1 when it is not.
on_crates_io() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' -A "push_release.sh (release version check)" \
    "https://crates.io/api/v1/crates/$1/$2")" || code="000"
  case "$code" in
    200) return 0 ;;
    404) return 1 ;;
    *) die "crates.io answered $code for $1 $2; try again later" ;;
  esac
}

remote_tag_exists() {
  git ls-remote --exit-code --tags "$REMOTE" "refs/tags/$1" >/dev/null 2>&1
}

workflow_for_tag() {
  case "$1" in
    client-v*) echo release-client.yml ;;
    server-v*) echo release-server.yml ;;
    v*) echo release-binaries.yml ;;
  esac
}

# The run a tag push started, once GitHub has registered it (up to 5 minutes).
run_for_tag() {
  local wf="$1" tag="$2" id=""
  for _ in $(seq 1 60); do
    id="$(gh run list -R "$GH_REPO" --workflow "$wf" --event push --limit 30 \
      --json databaseId,headBranch --jq ".[] | select(.headBranch == \"$tag\") | .databaseId" 2>/dev/null | head -n1)"
    [ -n "$id" ] && { echo "$id"; return 0; }
    sleep 5
  done
  return 1
}

wait_for_release() {
  local tag="$1" wf id
  wf="$(workflow_for_tag "$tag")"
  id="$(run_for_tag "$wf" "$tag")" || die "no $wf run appeared for $tag; see https://github.com/$GH_REPO/actions"
  note "$tag: https://github.com/$GH_REPO/actions/runs/$id"
  if gh run watch "$id" -R "$GH_REPO" --exit-status --interval 60 >/dev/null; then
    printf '    %s%s released%s\n' "$GREEN" "$tag" "$RESET"
  else
    die "the $tag release failed: https://github.com/$GH_REPO/actions/runs/$id (the tag stays; fix and re-run that workflow, or bump again)"
  fi
}

# ---------------------------------------------------------------- preflight

step "Checking"
gh auth status >/dev/null 2>&1 || die "gh is not logged in (gh auth login)"
GH_REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
git fetch --quiet --tags "$REMOTE" \
  "+refs/heads/$SOURCE:refs/remotes/$REMOTE/$SOURCE" \
  "+refs/heads/$BASE:refs/remotes/$REMOTE/$BASE" || die "could not fetch $SOURCE and $BASE from $REMOTE"
SRC="$REMOTE/$SOURCE"
SRC_SHA="$(git rev-parse "$SRC")"
note "releasing $SRC at $(git log -1 --format='%h %s' "$SRC")"
if git rev-parse --verify --quiet "refs/heads/$SOURCE" >/dev/null; then
  ahead="$(git rev-list --count "$SRC..refs/heads/$SOURCE")"
  [ "$ahead" = 0 ] || warn "your local $SOURCE has $ahead commit(s) that are not pushed; they are NOT in this release"
fi

# ---------------------------------------------------------------- plan

declare -A OLD=() NEW=()   # crate -> version, for the crates whose version changes
TAGS=()
CLIENT_VER=""; SERVER_VER=""; EMBEDDED_VER=""
CORE_NOW="$(version_at "$SRC" rasterizeddb_core)"

if [ "$CHANNEL" = client ] || [ "$CHANNEL" = all ]; then
  cur="$(version_at "$SRC" rasterizeddb_client)"
  CLIENT_VER="$(next_version "$cur" "$BUMP")"
  for c in "${CLIENT_CRATES[@]}"; do
    o="$(version_at "$SRC" "$c")"
    if [ "$o" != "$CLIENT_VER" ]; then OLD[$c]="$o"; NEW[$c]="$CLIENT_VER"; fi
  done
  TAGS+=("client-v$CLIENT_VER")
fi
if [ "$CHANNEL" = server ] || [ "$CHANNEL" = all ]; then
  SERVER_VER="$(next_version "$CORE_NOW" "$BUMP")"
  embedded_now="$(version_at "$SRC" rasterizeddb_embedded)"
  if [ -n "$EMBEDDED_BUMP" ]; then
    EMBEDDED_VER="$(next_version "$embedded_now" "$EMBEDDED_BUMP")"
  elif [[ "$BUMP" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] && [ "$embedded_now" != "$CORE_NOW" ]; then
    die "rasterizeddb_embedded is $embedded_now and rasterizeddb_core $CORE_NOW: with an explicit server version, name the SDK's too (--embedded X.Y.Z or a bump)"
  else
    EMBEDDED_VER="$(next_version "$embedded_now" "$BUMP")"
  fi
  for c in "${SERVER_CRATES[@]}"; do
    o="$(version_at "$SRC" "$c")"
    want="$SERVER_VER"; [ "$c" = rasterizeddb_embedded ] && want="$EMBEDDED_VER"
    if [ "$o" != "$want" ]; then OLD[$c]="$o"; NEW[$c]="$want"; fi
  done
  TAGS+=("server-v$SERVER_VER" "v$SERVER_VER")
  if [ "$CHANNEL" = server ]; then
    client_now="$(version_at "$SRC" rasterizeddb_client)"
    on_crates_io rasterizeddb_client "$client_now" \
      || die "rasterizeddb_client $client_now is not on crates.io yet and the embedded SDK needs it: release the client first, or use 'all'"
  fi
fi
if [ "$CHANNEL" = binaries ]; then
  remote_tag_exists "v$CORE_NOW" && die "v$CORE_NOW is already released; bump the server version first (./push_release.sh server patch)"
  if [ "$RC" = 1 ]; then
    n="$(git ls-remote --tags "$REMOTE" "refs/tags/v$CORE_NOW-rc.*" \
      | sed -n 's#.*refs/tags/v[0-9.]*-rc\.\([0-9]*\)$#\1#p' | sort -n | tail -n1)"
    TAGS+=("v$CORE_NOW-rc.$(( ${n:-0} + 1 ))")
  else
    TAGS+=("v$CORE_NOW")
  fi
fi

# rastcp: both crate channels publish it when its version is missing from
# crates.io. A version that is already there but whose code changed since the
# last crate release has to move, or the new crates would build against the old
# rastcp from crates.io.
if [ "$CHANNEL" != binaries ]; then
  rastcp_now="$(version_at "$SRC" "$SHARED_CRATE")"
  rastcp_new=""
  case "$RASTCP_BUMP" in
    keep) ;;
    auto)
      if on_crates_io "$SHARED_CRATE" "$rastcp_now"; then
        last="$(git tag --list 'client-v*' 'server-v*' --sort=-creatordate | head -n1)"
        if [ -z "$last" ]; then
          rastcp_new="$(next_version "$rastcp_now" patch)"
          note "rastcp $rastcp_now is on crates.io and no earlier crate release tag to compare with: bumping it a patch"
        elif ! git diff --quiet "$last" "$SRC" -- "$SHARED_CRATE"; then
          rastcp_new="$(next_version "$rastcp_now" patch)"
          note "rastcp changed since $last: bumping it a patch (--rastcp minor|major to say otherwise)"
        fi
      fi ;;
    *) rastcp_new="$(next_version "$rastcp_now" "$RASTCP_BUMP")" ;;
  esac
  if [ -n "$rastcp_new" ]; then OLD[$SHARED_CRATE]="$rastcp_now"; NEW[$SHARED_CRATE]="$rastcp_new"; fi
fi

for tag in "${TAGS[@]}"; do
  remote_tag_exists "$tag" && die "tag $tag already exists on $REMOTE"
done
for c in "${!NEW[@]}"; do
  if on_crates_io "$c" "${NEW[$c]}"; then die "$c ${NEW[$c]} is already on crates.io"; fi
done
if [ -n "$CLIENT_VER" ]; then
  for c in "${CLIENT_CRATES[@]}"; do
    if on_crates_io "$c" "$CLIENT_VER"; then die "$c $CLIENT_VER is already on crates.io; bump it"; fi
  done
fi
if [ -n "$SERVER_VER" ]; then
  if on_crates_io rasterizeddb_core "$SERVER_VER"; then die "rasterizeddb_core $SERVER_VER is already on crates.io; bump it"; fi
  if on_crates_io rasterizeddb_embedded "$EMBEDDED_VER"; then die "rasterizeddb_embedded $EMBEDDED_VER is already on crates.io; bump it (--embedded)"; fi
fi
# Not release/<tag>: a branch named `release` rules out any release/... ref.
BRANCH="release-pr/${TAGS[0]}"
# A run that stopped (a failed check, Ctrl+C) leaves its branch and pull
# request behind. A rerun takes both over: the branch is this script's own,
# so it is moved to the new head (with a lease on the commit seen here), and
# an open pull request from it is reused rather than opened again.
OLD_BRANCH_SHA="$(git ls-remote --heads "$REMOTE" "$BRANCH" | cut -f1)"
OPEN_PR=""
if [ -n "$OLD_BRANCH_SHA" ]; then
  OPEN_PR="$(gh pr list -R "$GH_REPO" --head "$BRANCH" --base "$BASE" --state open --json number --jq '.[0].number // empty')"
fi

step "Plan"
if [ ${#NEW[@]} -gt 0 ]; then
  for c in "${!NEW[@]}"; do note "$(printf '%-28s %s -> %s' "$c" "${OLD[$c]}" "${NEW[$c]}")"; done
else
  note "no version change"
fi
[ -n "$CLIENT_VER" ] && note "client crates release $CLIENT_VER"
[ -n "$SERVER_VER" ] && note "server crates release rasterizeddb_core $SERVER_VER and rasterizeddb_embedded $EMBEDDED_VER; executables $SERVER_VER"
[ "$CHANNEL" = binaries ] && note "executables of rasterizeddb_core $CORE_NOW"
true
note "tags:   ${TAGS[*]}"
note "flow:   $BRANCH -> pull request into $BASE -> checks -> merge -> $PUBLISHED fast-forwarded -> tags -> back into $SOURCE"
if [ -n "$OLD_BRANCH_SHA" ]; then
  note "rerun:  $BRANCH exists on $REMOTE at ${OLD_BRANCH_SHA:0:8}; it is moved to the new head${OPEN_PR:+ and pull request #$OPEN_PR is reused}"
fi
if [ "$CHANNEL" = binaries ] && [ "$RC" = 1 ]; then note "        (published as a GitHub pre-release)"; fi

if [ "$DRY_RUN" = 0 ] && [ "$YES" = 0 ]; then
  read -r -p "Proceed? [y/N] " answer
  case "$answer" in y|Y|yes) ;; *) die "stopped" ;; esac
fi

# ---------------------------------------------------------------- version commit

WT="$(mktemp -d "${TMPDIR:-/tmp}/push-release.XXXXXX")"
cleanup() { git -C "$REPO_DIR" worktree remove --force "$WT" >/dev/null 2>&1 || rm -rf "$WT"; }
trap cleanup EXIT
git worktree add --quiet --detach "$WT" "$SRC_SHA"

if [ ${#NEW[@]} -gt 0 ]; then
  step "Bumping versions"
  bumps=()
  for c in "${!NEW[@]}"; do bumps+=("$c=${OLD[$c]}=${NEW[$c]}"); done
  (cd "$WT" && python3 - "${bumps[@]}" <<'PY'
import re, subprocess, sys

bumps = {}
for arg in sys.argv[1:]:
    crate, old, new = arg.split("=")
    bumps[crate] = (old, new)

def files(pattern):
    out = subprocess.run(["git", "ls-files", pattern], capture_output=True, text=True, check=True)
    return [f for f in out.stdout.splitlines() if f]

def rewrite(path, fn):
    # newline="" keeps CRLF files CRLF.
    with open(path, encoding="utf-8", newline="") as f:
        text = f.read()
    new = fn(text)
    if new != text:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(new)
        print(f"    {path}")

# The [package] version of each bumped crate.
for crate, (old, new) in bumps.items():
    def package_version(text, old=old, new=new, crate=crate):
        m = re.search(r'(?ms)^\[package\]\s*$(.*?)(?=^\[|\Z)', text)
        section = m.group(1)
        fixed, n = re.subn(r'(?m)^(version\s*=\s*")' + re.escape(old) + '"', r'\g<1>' + new + '"', section, count=1)
        if n != 1:
            sys.exit(f"{crate}/Cargo.toml: no version = \"{old}\" under [package]")
        return text[:m.start(1)] + fixed + text[m.end(1):]
    rewrite(f"{crate}/Cargo.toml", package_version)

# Requirements on the bumped crates anywhere in the workspace:
#   name = { path = "...", version = "X" ... }
for path in files("*Cargo.toml"):
    def requirements(text):
        for crate, (_, new) in bumps.items():
            text = re.sub(
                r'(?m)^(\s*' + re.escape(crate) + r'\s*=\s*\{[^}\n]*?\bversion\s*=\s*")[^"]*(")',
                r'\g<1>' + new + r'\g<2>', text)
        return text
    rewrite(path, requirements)

# README pins such as  rasterizeddb_embedded = "1"  keep their precision.
for path in files("*.md"):
    def pins(text):
        for crate, (old, new) in bumps.items():
            def repl(m, old=old, new=new):
                parts = m.group(2).split(".")
                if old.split(".")[:len(parts)] != parts:
                    return m.group(0)
                return m.group(1) + ".".join(new.split(".")[:len(parts)]) + m.group(3)
            text = re.sub(r'(\b' + re.escape(crate) + r'\s*=\s*")(\d+(?:\.\d+){0,2})(")', repl, text)
        return text
    rewrite(path, pins)
PY
  ) || die "the version bump failed"

  (cd "$WT" && cargo update --workspace --quiet) || die "cargo update --workspace failed"
  for c in "${!NEW[@]}"; do
    got="$(cd "$WT" && cargo pkgid -p "$c" | sed 's/.*[#@]//')"
    [ "$got" = "${NEW[$c]}" ] || die "$c is $got after the bump, expected ${NEW[$c]}"
  done
  (cd "$WT" && cargo metadata --locked --format-version 1 >/dev/null) \
    || die "Cargo.lock is not consistent after the bump"

  summary=""
  for c in "${!NEW[@]}"; do summary+="$c ${NEW[$c]}, "; done
  git -C "$WT" add -A
  git -C "$WT" commit --quiet -m "chore(release): ${TAGS[*]}" -m "Versions: ${summary%, }."
  git -C "$WT" --no-pager show --stat --format='    %h %s' HEAD | sed 's/^/    /'
fi
HEAD_SHA="$(git -C "$WT" rev-parse HEAD)"

if [ "$DRY_RUN" = 1 ]; then
  if [ ${#NEW[@]} -gt 0 ]; then git -C "$WT" --no-pager diff HEAD~1 -- '*Cargo.toml' '*.md'; fi
  step "Dry run: nothing was pushed, tagged or merged"
  exit 0
fi

# ---------------------------------------------------------------- pull request

step "Pull request into $BASE"
git -C "$WT" push --quiet --force-with-lease="refs/heads/$BRANCH:$OLD_BRANCH_SHA" "$REMOTE" "HEAD:refs/heads/$BRANCH" \
  || die "could not push $BRANCH (did it move on $REMOTE since the check? rerun)"
if [ "$(git rev-list --count "$REMOTE/$BASE..$HEAD_SHA")" = 0 ]; then
  note "$BASE already contains $SOURCE; no pull request needed"
  MERGE_SHA="$HEAD_SHA"
else
  body="Release ${TAGS[*]} from \`$SOURCE\` at $(git rev-parse --short "$SRC_SHA"), opened by push_release.sh.

Once every check passes, the script merges this, tags the merge commit (${TAGS[*]}), and brings the version commit back to \`$SOURCE\`."
  if [ -n "$OPEN_PR" ]; then
    PR="$OPEN_PR"
    PR_URL="$(gh pr view "$PR" -R "$GH_REPO" --json url --jq .url)"
    gh pr edit "$PR" -R "$GH_REPO" --title "Release ${TAGS[*]}" --body "$body" >/dev/null || true
    note "$PR_URL (reused)"
  else
    PR_URL="$(gh pr create -R "$GH_REPO" --base "$BASE" --head "$BRANCH" --title "Release ${TAGS[*]}" --body "$body")"
    PR="${PR_URL##*/}"
    note "$PR_URL"
  fi

  # The gate is this pull request's own runs: pull_request events from
  # $BRANCH at exactly $HEAD_SHA. Not `gh pr checks`, which lists every check
  # on the head commit: when the head is a tip CI already ran on (a release
  # with no version change), that includes the push runs and their full
  # suites, which a pull request never runs, and an old failure there stopped
  # the release while the pull request's own checks were still going.
  pr_runs() {
    gh run list -R "$GH_REPO" --event pull_request --commit "$HEAD_SHA" --branch "$BRANCH" --limit 50 \
      --json databaseId,workflowName,status,conclusion,url \
      --jq 'group_by(.workflowName) | map(max_by(.databaseId)) | .[] | [.databaseId, .status, (if .conclusion == "" then "-" else .conclusion end), .workflowName, .url] | @tsv'
  }

  note "waiting for the pull request's runs to start"
  runs=""
  for _ in $(seq 1 60); do
    runs="$(pr_runs 2>/dev/null || true)"
    [ -n "$runs" ] && break
    sleep 10
  done
  [ -n "$runs" ] || die "no pull_request runs started for $HEAD_SHA within 10 minutes; the pull request is left open: $PR_URL"
  # Workflows register a little apart; give the slower ones a moment.
  sleep 30
  # A rerun on the same head pushes nothing, so no run starts: the runs at
  # that commit are the ones that stopped the last attempt. Re-run their
  # failed jobs once, rather than stopping again on the same failure.
  if [ "$OLD_BRANCH_SHA" = "$HEAD_SHA" ]; then
    rerun=0
    while IFS=$'\t' read -r id status conclusion name _; do
      [ -n "$id" ] && [ "$status" = completed ] || continue
      case "$conclusion" in
        success|skipped|neutral) ;;
        *) note "re-running the failed jobs of $name ($conclusion), from the last attempt"
           gh run rerun "$id" -R "$GH_REPO" --failed >/dev/null || die "could not re-run $name: gh run rerun $id --failed"
           rerun=1 ;;
      esac
    done <<<"$(pr_runs)"
    if [ "$rerun" = 1 ]; then sleep 30; fi
  fi

  note "waiting for them to finish (the PGO executable build takes about 40 minutes)"
  while :; do
    runs="$(pr_runs)" || die "could not list the runs for $PR_URL"
    pending=0 failed=""
    while IFS=$'\t' read -r id status conclusion name url; do
      [ -n "$id" ] || continue
      if [ "$status" != completed ]; then pending=1; continue; fi
      case "$conclusion" in
        success|skipped|neutral) ;;
        *) failed+="$name ($conclusion): $url  (gh run rerun $id --failed)"$'\n' ;;
      esac
    done <<<"$runs"
    if [ -n "$failed" ]; then
      printf '%s' "$failed" | sed 's/^/    /' >&2
      die "a check failed; nothing was merged or tagged. The pull request stays open: $PR_URL. Fix and run the script again; with the head unchanged, it re-runs the failed jobs itself."
    fi
    [ "$pending" = 0 ] && break
    sleep 60
  done
  while IFS=$'\t' read -r _ _ _ name _; do note "passed: $name"; done <<<"$runs"
  note "all checks passed"

  gh pr merge "$PR" -R "$GH_REPO" --merge --match-head-commit "$HEAD_SHA" \
    --subject "Release ${TAGS[*]} (#$PR)" --body "From $SOURCE at $(git rev-parse --short "$SRC_SHA")." \
    || die "could not merge $PR_URL (branch protection, or a conflict with $BASE?); nothing was tagged"
  MERGE_SHA=""
  for _ in $(seq 1 12); do
    MERGE_SHA="$(gh pr view "$PR" -R "$GH_REPO" --json mergeCommit --jq '.mergeCommit.oid // empty')"
    [ -n "$MERGE_SHA" ] && break
    sleep 5
  done
  [ -n "$MERGE_SHA" ] || die "merged, but GitHub did not report the merge commit; tag it by hand"
  note "merged as $(git rev-parse --short "$MERGE_SHA" 2>/dev/null || echo "$MERGE_SHA")"
fi
git push --quiet "$REMOTE" --delete "$BRANCH" 2>/dev/null || true
git fetch --quiet "$REMOTE" "+refs/heads/$BASE:refs/remotes/$REMOTE/$BASE" "+refs/heads/$SOURCE:refs/remotes/$REMOTE/$SOURCE"
git merge-base --is-ancestor "$MERGE_SHA" "$REMOTE/$BASE" || die "$MERGE_SHA is not on $REMOTE/$BASE"

# ---------------------------------------------------------------- main follows release

step "$PUBLISHED fast-forwarded to $BASE"
git fetch --quiet "$REMOTE" "+refs/heads/$PUBLISHED:refs/remotes/$REMOTE/$PUBLISHED"
if git merge-base --is-ancestor "$MERGE_SHA" "$REMOTE/$PUBLISHED"; then
  note "$PUBLISHED already has $(git rev-parse --short "$MERGE_SHA")"
elif git merge-base --is-ancestor "$REMOTE/$PUBLISHED" "$MERGE_SHA"; then
  git push --quiet "$REMOTE" "$MERGE_SHA:refs/heads/$PUBLISHED" \
    || die "could not fast-forward $PUBLISHED (branch protection?); nothing was tagged. Push $MERGE_SHA to $PUBLISHED, then rerun"
  note "$PUBLISHED is at $(git rev-parse --short "$MERGE_SHA")"
else
  die "$PUBLISHED has commits that $BASE does not; only releases may reach $PUBLISHED. Nothing was tagged: merge $PUBLISHED into $SOURCE, then rerun"
fi

# ---------------------------------------------------------------- back into the source branch

if [ ${#NEW[@]} -gt 0 ]; then
  step "Version commit back into $SOURCE"
  back=0
  for _ in 1 2 3; do
    git fetch --quiet "$REMOTE" "+refs/heads/$SOURCE:refs/remotes/$REMOTE/$SOURCE"
    if git merge-base --is-ancestor "$REMOTE/$SOURCE" "$MERGE_SHA"; then
      git push --quiet "$REMOTE" "$MERGE_SHA:refs/heads/$SOURCE" && { back=1; note "fast-forwarded $SOURCE to $(git rev-parse --short "$MERGE_SHA")"; break; }
    else
      git -C "$WT" checkout --quiet --detach "$REMOTE/$SOURCE"
      if ! git -C "$WT" merge --quiet --no-ff --no-edit -m "Merge release ${TAGS[*]} back into $SOURCE" "$MERGE_SHA"; then
        git -C "$WT" merge --abort || true
        warn "the version commit conflicts with newer work on $SOURCE; merge $REMOTE/$BASE into $SOURCE by hand"
        break
      fi
      git -C "$WT" push --quiet "$REMOTE" "HEAD:refs/heads/$SOURCE" && { back=1; note "merged into $SOURCE as $(git -C "$WT" rev-parse --short HEAD)"; break; }
    fi
    note "$SOURCE moved while pushing; trying again"
  done
  if [ "$back" = 1 ]; then
    note "other checkouts of $SOURCE need a pull before their next commit"
  else
    warn "$SOURCE does not have the version commit yet; the next release would bump from the old versions until it does"
  fi
fi

# ---------------------------------------------------------------- tags

step "Tags on $BASE (and $PUBLISHED)"
for tag in "${TAGS[@]}"; do
  # The server crates need the client version on crates.io.
  if [ -n "$CLIENT_VER" ] && [ "$tag" = "server-v$SERVER_VER" ]; then
    note "waiting for the client release before the server tags"
    wait_for_release "client-v$CLIENT_VER"
  fi
  git tag -a "$tag" "$MERGE_SHA" -m "$tag"
  git push --quiet "$REMOTE" "refs/tags/$tag"
  note "pushed $tag"
done

if [ "$WAIT" = 1 ]; then
  step "Release workflows"
  for tag in "${TAGS[@]}"; do
    if [ "$CHANNEL" = all ] && [ "$tag" = "client-v$CLIENT_VER" ]; then continue; fi  # already waited for
    wait_for_release "$tag"
  done
  step "Released"
  for tag in "${TAGS[@]}"; do
    note "$(gh release view "$tag" -R "$GH_REPO" --json url --jq .url 2>/dev/null || echo "$tag (no GitHub release found)")"
  done
else
  step "Tags pushed; the release workflows are running: https://github.com/$GH_REPO/actions"
fi
