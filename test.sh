#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$ROOT_DIR/artifacts/api_story_test"
RUNTIME_DIR="$BUILD_DIR/runtime"
BIN="$BUILD_DIR/api_story_test"
LOG="$BUILD_DIR/output.log"
CC_BIN="${CC:-cc}"

if [[ "${1:-}" == "--clean" ]]; then
  rm -rf "$BUILD_DIR"
  echo "[clean] removed $BUILD_DIR"
  exit 0
fi

mkdir -p "$BUILD_DIR"
rm -rf "$RUNTIME_DIR"
mkdir -p "$RUNTIME_DIR"

echo "[build] sqlsprocessor baseline"
make -C "$ROOT_DIR" build

echo "[build] API story test runner"
"$CC_BIN" -O2 \
  -DTCP_MAX_CONNECTIONS_PER_CLIENT=4 \
  -DTCP_MAX_INFLIGHT_PER_CONNECTION=16 \
  -DTCP_MAX_INFLIGHT_PER_CLIENT=64 \
  -DTCP_READ_TIMEOUT_MS=5000 \
  -DTCP_WRITE_TIMEOUT_MS=5000 \
  -I"$ROOT_DIR" \
  -I"$ROOT_DIR/cmd_processor" \
  -I"$ROOT_DIR/thirdparty/cjson" \
  "$ROOT_DIR/tests/api_story_test.c" \
  "$ROOT_DIR/cmd_processor/tcp_cmd_processor.c" \
  "$ROOT_DIR/cmd_processor/cmd_processor.c" \
  "$ROOT_DIR/cmd_processor/engine_cmd_processor_bundle.c" \
  "$ROOT_DIR/lexer.c" \
  "$ROOT_DIR/parser.c" \
  "$ROOT_DIR/bptree.c" \
  "$ROOT_DIR/jungle_benchmark.c" \
  "$ROOT_DIR/executor.c" \
  "$ROOT_DIR/thirdparty/cjson/cJSON.c" \
  -o "$BIN" \
  -pthread

echo "[run] API story test"
(
  cd "$RUNTIME_DIR"
  "$BIN" "$@"
) | tee "$LOG"

echo
echo "[done] log saved to $LOG"
echo "[done] runtime files saved to $RUNTIME_DIR"
