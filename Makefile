CC ?= gcc
CFLAGS ?= -fdiagnostics-color=always -g
TARGET ?= sqlsprocessor
BENCH_GEN ?= bench_workload_generator
BENCH_RUNNER ?= benchmark_runner
BENCH_TEST ?= bench_formula_test
SRC = main.c
SRC_DEPS = main.c lexer.c parser.c bptree.c executor.c lexer.h parser.h bptree.h executor.h types.h
SQL ?= demo_bptree.sql
PYTHON ?= python
JUNGLE_DATASET ?= jungle_benchmark_users.csv
JUNGLE_RECORDS ?= 1000000

.PHONY: all build bench-tools bench-test run demo-bptree demo-jungle scenario-jungle-regression scenario-jungle-range-and-replay scenario-jungle-update-constraints generate-jungle generate-jungle-sql benchmark benchmark-jungle bench-smoke bench-score bench-report bench-clean clean

all: build

build: $(TARGET)

$(TARGET): $(SRC_DEPS)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

bench-tools: $(BENCH_GEN) $(BENCH_RUNNER)

$(BENCH_GEN): bench_workload_generator.c
	$(CC) $(CFLAGS) bench_workload_generator.c -o $(BENCH_GEN)

$(BENCH_RUNNER): benchmark_runner.c
	$(CC) $(CFLAGS) benchmark_runner.c -o $(BENCH_RUNNER)

$(BENCH_TEST): bench_formula_test.c
	$(CC) $(CFLAGS) bench_formula_test.c -o $(BENCH_TEST)

bench-test: $(BENCH_TEST)
	./$(BENCH_TEST)

$(JUNGLE_DATASET): $(TARGET)
	./$(TARGET) --generate-jungle $(JUNGLE_RECORDS) $(JUNGLE_DATASET)

run: $(TARGET)
	./$(TARGET) $(SQL)

demo-bptree: $(TARGET)
	./$(TARGET) demo_bptree.sql

demo-jungle: $(TARGET) $(JUNGLE_DATASET)
	./$(TARGET) demo_jungle.sql

scenario-jungle-regression: $(TARGET)
	./$(TARGET) scenario_jungle_regression.sql

scenario-jungle-range-and-replay: $(TARGET)
	./$(TARGET) scenario_jungle_range_and_replay.sql

scenario-jungle-update-constraints: $(TARGET)
	./$(TARGET) scenario_jungle_update_constraints.sql

generate-jungle: $(JUNGLE_DATASET)

generate-jungle-sql: $(JUNGLE_DATASET)
	$(PYTHON) scripts/generate_jungle_sql_workloads.py

benchmark: $(TARGET)
	./$(TARGET) --benchmark 1000000

benchmark-jungle: $(TARGET)
	./$(TARGET) --benchmark-jungle 1000000

bench-smoke: build bench-tools
	./$(BENCH_RUNNER) --profile smoke --seed 20260415 --repeat 3

bench-score: build bench-tools
	./$(BENCH_RUNNER) --profile score --seed 20260415 --repeat 1

bench-report: $(BENCH_RUNNER)
	./$(BENCH_RUNNER) --report-only

bench-clean:
	rm -rf artifacts/bench
	rm -f generated_sql/jungle_insert_smoke.sql generated_sql/jungle_update_smoke.sql generated_sql/jungle_delete_smoke.sql
	rm -f generated_sql/jungle_insert_regression.sql generated_sql/jungle_update_regression.sql generated_sql/jungle_delete_regression.sql
	rm -f generated_sql/jungle_insert_score.sql generated_sql/jungle_update_score.sql generated_sql/jungle_delete_score.sql
	rm -f generated_sql/jungle_correctness_success_smoke.sql generated_sql/jungle_correctness_failure_smoke.sql
	rm -f generated_sql/jungle_correctness_success_regression.sql generated_sql/jungle_correctness_failure_regression.sql
	rm -f generated_sql/jungle_correctness_success_score.sql generated_sql/jungle_correctness_failure_score.sql
	rm -f generated_sql/workload_smoke.sql generated_sql/workload_regression.sql generated_sql/workload_score.sql
	rm -f generated_sql/workload_smoke.meta.json generated_sql/workload_regression.meta.json generated_sql/workload_score.meta.json
	rm -f generated_sql/oracle_smoke.json generated_sql/oracle_regression.json generated_sql/oracle_score.json

clean:
	rm -f $(TARGET) $(BENCH_GEN) $(BENCH_RUNNER) $(BENCH_TEST)
