#!/bin/sh
set -eu

repo_dir=$1
update_rows=$2
delete_rows=$3
tmp_root=${TMPDIR:-/tmp}
bench_dir="$tmp_root/sqlprocessor-bench-$$"

cleanup() {
    rm -rf "$bench_dir"
}
trap cleanup EXIT INT TERM

mkdir -p "$bench_dir"
mkdir -p "$repo_dir/artifacts/bench"

for file in \
    Makefile \
    main.c lexer.c parser.c bptree.c executor.c \
    lexer.h parser.h bptree.h executor.h types.h \
    bench_workload_generator.c benchmark_runner.c bench_formula_test.c
do
    cp "$repo_dir/$file" "$bench_dir/$file"
done

if [ -f "$repo_dir/jungle_benchmark_users.csv" ]; then
    cp "$repo_dir/jungle_benchmark_users.csv" "$bench_dir/jungle_benchmark_users.csv"
fi

(
    cd "$bench_dir"
    make bench-score \
        BENCH_SCORE_IN_TMP=0 \
        BENCH_SCORE_UPDATE_ROWS="$update_rows" \
        BENCH_SCORE_DELETE_ROWS="$delete_rows"
)

rm -rf "$repo_dir/artifacts/bench"
mkdir -p "$repo_dir/artifacts/bench"
cp -R "$bench_dir/artifacts/bench/." "$repo_dir/artifacts/bench/"
