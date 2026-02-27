#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/branch_regression.sh [REPO_PATH]

Environment variables:
  BASE_BRANCH   Baseline branch name (default: main)
  NEW_BRANCH    Candidate branch name (default: shared-buffer)
  Z3_ROOT       Z3 install prefix (default: /opt/z3-4.8.11)
  OUT_DIR       Output directory (default: /tmp/fperf-export-reg-<timestamp>)

Examples:
  scripts/branch_regression.sh /Users/wenkaili/fperf
  BASE_BRANCH=main NEW_BRANCH=feature-x scripts/branch_regression.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

REPO="${1:-$PWD}"
BASE_BRANCH="${BASE_BRANCH:-main}"
NEW_BRANCH="${NEW_BRANCH:-shared-buffer}"
Z3_ROOT="${Z3_ROOT:-/opt/z3-4.8.11}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-/tmp/fperf-export-reg-$TS}"

if ! command -v git >/dev/null 2>&1; then
    echo "ERROR: git not found in PATH"
    exit 1
fi

if ! command -v make >/dev/null 2>&1; then
    echo "ERROR: make not found in PATH"
    exit 1
fi

if [[ ! -d "$REPO/.git" ]]; then
    echo "ERROR: '$REPO' is not a git repository root"
    exit 1
fi

git -C "$REPO" rev-parse --verify "$BASE_BRANCH" >/dev/null
git -C "$REPO" rev-parse --verify "$NEW_BRANCH" >/dev/null

mkdir -p "$OUT_DIR/src-$BASE_BRANCH" "$OUT_DIR/src-$NEW_BRANCH"

export_branch() {
    local br="$1"
    local dst="$OUT_DIR/src-$br"
    git -C "$REPO" archive "$br" | tar -x -C "$dst"
}

run_branch() {
    local br="$1"
    local src="$OUT_DIR/src-$br"
    local build_rc=0
    local test_rc=0

    (cd "$src" && make clean >"$OUT_DIR/$br.clean.log" 2>&1) || true

    (cd "$src" && make Z3_ROOT="$Z3_ROOT" >"$OUT_DIR/$br.build.log" 2>&1) || build_rc=$?

    if [[ "$build_rc" -eq 0 ]]; then
        (cd "$src" && make test Z3_ROOT="$Z3_ROOT" >"$OUT_DIR/$br.test.log" 2>&1) || test_rc=$?
    else
        test_rc=125
        echo "build failed, test skipped" >"$OUT_DIR/$br.test.log"
    fi

    echo "$build_rc" >"$OUT_DIR/$br.build.rc"
    echo "$test_rc" >"$OUT_DIR/$br.test.rc"

    {
        echo "BUILD_EXIT_CODE=$build_rc"
        echo "TEST_EXIT_CODE=$test_rc"
        grep -E "Running:|Pass|Fail|UNSAT|SAT|Test Results" "$OUT_DIR/$br.test.log" || true
    } >"$OUT_DIR/$br.summary.txt"
}

export_branch "$BASE_BRANCH"
export_branch "$NEW_BRANCH"

run_branch "$BASE_BRANCH"
run_branch "$NEW_BRANCH"

echo "===== EXIT CODE ====="
echo "$BASE_BRANCH: build=$(cat "$OUT_DIR/$BASE_BRANCH.build.rc"), test=$(cat "$OUT_DIR/$BASE_BRANCH.test.rc")"
echo "$NEW_BRANCH: build=$(cat "$OUT_DIR/$NEW_BRANCH.build.rc"), test=$(cat "$OUT_DIR/$NEW_BRANCH.test.rc")"

echo "===== DIFF (summary) ====="
if diff -u "$OUT_DIR/$BASE_BRANCH.summary.txt" "$OUT_DIR/$NEW_BRANCH.summary.txt" >"$OUT_DIR/summary.diff"; then
    echo "[NO DIFF] summaries are identical"
else
    cat "$OUT_DIR/summary.diff"
fi

echo "===== LOG DIR ====="
echo "$OUT_DIR"

base_build_rc="$(cat "$OUT_DIR/$BASE_BRANCH.build.rc")"
base_test_rc="$(cat "$OUT_DIR/$BASE_BRANCH.test.rc")"
new_build_rc="$(cat "$OUT_DIR/$NEW_BRANCH.build.rc")"
new_test_rc="$(cat "$OUT_DIR/$NEW_BRANCH.test.rc")"

if [[ "$base_build_rc" -ne 0 || "$base_test_rc" -ne 0 || "$new_build_rc" -ne 0 || "$new_test_rc" -ne 0 ]]; then
    exit 1
fi

if [[ -s "$OUT_DIR/summary.diff" ]]; then
    exit 2
fi

exit 0
