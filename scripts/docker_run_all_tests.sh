#!/usr/bin/env sh
set -eu

cd /app

make clean
make build
make test-cmd-processor
make test-tcp-cmd-processor
make test-repl-cmd-processor

if [ "${RUN_SCALE_SCORE:-0}" = "1" ]; then
  make test-cmd-processor-scale-score
fi
