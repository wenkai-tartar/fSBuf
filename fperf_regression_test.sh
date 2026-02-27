#!/usr/bin/env bash
set -u

REPO="${1:-$PWD}"
Z3_ROOT="${Z3_ROOT:-/opt/z3-4.8.11}"
BASE_BRANCH="${BASE_BRANCH:-main}"
NEW_BRANCH="${NEW_BRANCH:-shared-buffer}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="/tmp/fperf-export-reg-$TS"

mkdir -p "$OUT"

export_branch() {
  local br="$1"
  local dir="$OUT/src-$br"
  mkdir -p "$dir"
  git -C "$REPO" archive "$br" | tar -x -C "$dir"
}

run_branch() {
  local br="$1"
  local dir="$OUT/src-$br"
  local build_rc=0
  local test_rc=0

  (cd "$dir" && make clean >"$OUT/$br.clean.log" 2>&1) || true

  (cd "$dir" && make Z3_ROOT="$Z3_ROOT" >"$OUT/$br.build.log" 2>&1)
  build_rc=$?

  if [[ "$build_rc" -eq 0 ]]; then
    (cd "$dir" && make test Z3_ROOT="$Z3_ROOT" >"$OUT/$br.test.log" 2>&1)
    test_rc=$?
  else
    test_rc=125
    echo "build failed, test skipped" >"$OUT/$br.test.log"
  fi

  echo "$build_rc" >"$OUT/$br.build.rc"
  echo "$test_rc" >"$OUT/$br.test.rc"

  {
    echo "BRANCH=$br"
    echo "BUILD_EXIT_CODE=$build_rc"
    echo "TEST_EXIT_CODE=$test_rc"
    echo "--- KEY LOG LINES ---"
    grep -E "Running:|Pass|Fail|UNSAT|SAT|Test Results" "$OUT/$br.test.log" || true
  } >"$OUT/$br.summary.txt"
}

# 1) 导出
export_branch "$BASE_BRANCH"
export_branch "$NEW_BRANCH"

# 2) 分别构建测试
run_branch "$BASE_BRANCH"
run_branch "$NEW_BRANCH"

# 3) 明确打印 exit code
echo "===== EXIT CODE ====="
echo "$BASE_BRANCH: build=$(cat "$OUT/$BASE_BRANCH.build.rc"), test=$(cat "$OUT/$BASE_BRANCH.test.rc")"
echo "$NEW_BRANCH: build=$(cat "$OUT/$NEW_BRANCH.build.rc"), test=$(cat "$OUT/$NEW_BRANCH.test.rc")"

echo "===== DIFF (summary) ====="
if diff -u "$OUT/$BASE_BRANCH.summary.txt" "$OUT/$NEW_BRANCH.summary.txt" >"$OUT/summary.diff"; then
  echo "[NO DIFF] summaries are identical"
else
  cat "$OUT/summary.diff"
fi

echo "===== LOG DIR ====="
echo "$OUT"
