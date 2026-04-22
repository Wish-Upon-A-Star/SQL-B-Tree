CC ?= gcc
CFLAGS ?= -O2 -fdiagnostics-color=always -g
TARGET ?= sqlsprocessor
TCP_SERVER_TARGET ?= tcp_sql_server
STRESS_TARGET ?= stress_runner
BENCH_GEN ?= bench_workload_generator
BENCH_RUNNER ?= benchmark_runner
BENCH_TEST ?= bench_formula_test
CMD_PROCESSOR_TEST ?= cmd_processor_test
ENGINE_CMD_PROCESSOR_TEST ?= engine_cmd_processor_test
TCP_CMD_PROCESSOR_TEST ?= tcp_cmd_processor_test
REPL_CMD_PROCESSOR_TEST ?= repl_cmd_processor_test
CMD_PROCESSOR_SCALE_SCORE_TEST ?= cmd_processor_engine_scale_score_test
CMD_PROCESSOR_DIR = cmd_processor
CJSON_DIR = thirdparty/cjson
SRC = main.c
TCP_SERVER_SRC = tcp_server_main.c
STRESS_SRC = stress_main.c
SRC_DEPS = main.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c bench_memtrack.h jungle_benchmark.h lexer.h parser.h bptree.h executor.h types.h sqlsprocessor_bundle.h platform_threads.h $(CMD_PROCESSOR_DIR)/cmd_processor.h $(CMD_PROCESSOR_DIR)/cmd_processor_sync_bridge.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor_internal.h $(CMD_PROCESSOR_DIR)/cmd_processor.c $(CMD_PROCESSOR_DIR)/cmd_processor_sync_bridge.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_support.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_planner.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_runtime.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor.c
TCP_SERVER_DEPS = tcp_server_main.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c bench_memtrack.h jungle_benchmark.h lexer.h parser.h bptree.h executor.h types.h platform_threads.h $(CMD_PROCESSOR_DIR)/cmd_processor.h $(CMD_PROCESSOR_DIR)/cmd_processor_sync_bridge.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor_internal.h $(CMD_PROCESSOR_DIR)/tcp_cmd_processor.h $(CMD_PROCESSOR_DIR)/cmd_processor.c $(CMD_PROCESSOR_DIR)/cmd_processor_sync_bridge.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_support.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_planner.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_runtime.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor.c $(CMD_PROCESSOR_DIR)/tcp_cmd_processor.c $(CJSON_DIR)/cJSON.c $(CJSON_DIR)/cJSON.h
STRESS_DEPS = stress_main.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c bench_memtrack.h jungle_benchmark.h lexer.h parser.h bptree.h executor.h types.h platform_threads.h
CMD_PROCESSOR_TEST_SRC = $(CMD_PROCESSOR_DIR)/cmd_processor_test.c $(CMD_PROCESSOR_DIR)/cmd_processor.c $(CMD_PROCESSOR_DIR)/mock_cmd_processor.c
CMD_PROCESSOR_TEST_DEPS = $(CMD_PROCESSOR_TEST_SRC) $(CMD_PROCESSOR_DIR)/cmd_processor.h $(CMD_PROCESSOR_DIR)/mock_cmd_processor.h
CMD_PROCESSOR_TEST_RUN = $(if $(filter /%,$(CMD_PROCESSOR_TEST)),$(CMD_PROCESSOR_TEST),./$(CMD_PROCESSOR_TEST))
ENGINE_CMD_PROCESSOR_TEST_SRC = $(CMD_PROCESSOR_DIR)/engine_cmd_processor_test.c $(CMD_PROCESSOR_DIR)/cmd_processor.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_bundle.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c
ENGINE_CMD_PROCESSOR_TEST_DEPS = $(ENGINE_CMD_PROCESSOR_TEST_SRC) $(CMD_PROCESSOR_DIR)/cmd_processor.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor_internal.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor_support.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_planner.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_runtime.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_test_support.h executor.h parser.h types.h platform_threads.h
ENGINE_CMD_PROCESSOR_TEST_RUN = $(if $(filter /%,$(ENGINE_CMD_PROCESSOR_TEST)),$(ENGINE_CMD_PROCESSOR_TEST),./$(ENGINE_CMD_PROCESSOR_TEST))
TCP_CMD_PROCESSOR_TEST_SRC = $(CMD_PROCESSOR_DIR)/tcp_cmd_processor_test.c $(CMD_PROCESSOR_DIR)/tcp_cmd_processor.c $(CMD_PROCESSOR_DIR)/cmd_processor.c $(CMD_PROCESSOR_DIR)/mock_cmd_processor.c $(CJSON_DIR)/cJSON.c
TCP_CMD_PROCESSOR_TEST_DEPS = $(TCP_CMD_PROCESSOR_TEST_SRC) $(CMD_PROCESSOR_DIR)/tcp_cmd_processor.h $(CMD_PROCESSOR_DIR)/cmd_processor.h $(CMD_PROCESSOR_DIR)/mock_cmd_processor.h $(CJSON_DIR)/cJSON.h
TCP_CMD_PROCESSOR_TEST_CFLAGS = -DTCP_MAX_CONNECTIONS_TOTAL=4 -DTCP_MAX_CONNECTIONS_PER_CLIENT=2 -DTCP_MAX_INFLIGHT_PER_CONNECTION=2 -DTCP_MAX_INFLIGHT_PER_CLIENT=3
TCP_CMD_PROCESSOR_TEST_RUN = $(if $(filter /%,$(TCP_CMD_PROCESSOR_TEST)),$(TCP_CMD_PROCESSOR_TEST),./$(TCP_CMD_PROCESSOR_TEST))
REPL_CMD_PROCESSOR_TEST_SRC = $(CMD_PROCESSOR_DIR)/repl_cmd_processor_test.c $(CMD_PROCESSOR_DIR)/repl_cmd_processor.c $(CMD_PROCESSOR_DIR)/sql_repl_engine.c $(CMD_PROCESSOR_DIR)/cmd_processor.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c
REPL_CMD_PROCESSOR_TEST_DEPS = $(REPL_CMD_PROCESSOR_TEST_SRC) $(CMD_PROCESSOR_DIR)/repl_cmd_processor.h $(CMD_PROCESSOR_DIR)/sql_repl_engine.h $(CMD_PROCESSOR_DIR)/cmd_processor.h executor.h parser.h types.h platform_threads.h
REPL_CMD_PROCESSOR_TEST_RUN = $(if $(filter /%,$(REPL_CMD_PROCESSOR_TEST)),$(REPL_CMD_PROCESSOR_TEST),./$(REPL_CMD_PROCESSOR_TEST))
CMD_PROCESSOR_SCALE_SCORE_TEST_SRC = $(CMD_PROCESSOR_DIR)/engine_cmd_processor_scale_score_test.c $(CMD_PROCESSOR_DIR)/cmd_processor.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_bundle.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c
CMD_PROCESSOR_SCALE_SCORE_TEST_DEPS = $(CMD_PROCESSOR_SCALE_SCORE_TEST_SRC) $(CMD_PROCESSOR_DIR)/cmd_processor.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor_internal.h $(CMD_PROCESSOR_DIR)/engine_cmd_processor_support.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_planner.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_runtime.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_test_support.h executor.h parser.h types.h platform_threads.h
CMD_PROCESSOR_SCALE_SCORE_TEST_RUN = $(if $(filter /%,$(CMD_PROCESSOR_SCALE_SCORE_TEST)),$(CMD_PROCESSOR_SCALE_SCORE_TEST),./$(CMD_PROCESSOR_SCALE_SCORE_TEST))
SQL ?= demo_bptree.sql
PYTHON ?= python
JUNGLE_DATASET ?= jungle_benchmark_users.csv
JUNGLE_RECORDS ?= 1000000
BENCH_SCORE_UPDATE_ROWS ?= 1000000
BENCH_SCORE_DELETE_ROWS ?= 1000000
BENCH_SCORE_IN_TMP ?= 1
BENCH_EXEC_SPEC ?= bench_score_exec.conf

.PHONY: all build tcp-server stress-tools bench-tools bench-test test-cmd-processor test-engine-cmd-processor test-tcp-cmd-processor test-repl-cmd-processor test-cmd-processor-scale-score docker-build docker-test docker-test-scale run demo-bptree demo-jungle scenario-jungle-regression scenario-jungle-range-and-replay scenario-jungle-update-constraints generate-jungle generate-jungle-sql benchmark benchmark-jungle bench-smoke bench-score bench-report bench-clean clean

all: build

build: $(TARGET)

$(TARGET): $(SRC_DEPS)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

tcp-server: $(TCP_SERVER_TARGET)

$(TCP_SERVER_TARGET): $(TCP_SERVER_DEPS)
	$(CC) $(CFLAGS) -I$(CMD_PROCESSOR_DIR) -I$(CJSON_DIR) $(TCP_SERVER_SRC) lexer.c parser.c bptree.c jungle_benchmark.c executor.c $(CMD_PROCESSOR_DIR)/cmd_processor.c $(CMD_PROCESSOR_DIR)/cmd_processor_sync_bridge.c $(CMD_PROCESSOR_DIR)/engine_cmd_processor_bundle.c $(CMD_PROCESSOR_DIR)/tcp_cmd_processor.c $(CJSON_DIR)/cJSON.c -o $(TCP_SERVER_TARGET) -pthread

stress-tools: $(STRESS_TARGET)

$(STRESS_TARGET): $(STRESS_DEPS)
	$(CC) $(CFLAGS) $(STRESS_SRC) lexer.c parser.c bptree.c jungle_benchmark.c executor.c -o $(STRESS_TARGET)

bench-tools: $(BENCH_GEN) $(BENCH_RUNNER) $(STRESS_TARGET)

$(BENCH_GEN): bench_workload_generator.c
	$(CC) $(CFLAGS) bench_workload_generator.c -o $(BENCH_GEN)

$(BENCH_RUNNER): benchmark_runner.c
	$(CC) $(CFLAGS) benchmark_runner.c -o $(BENCH_RUNNER)

$(BENCH_TEST): bench_formula_test.c
	$(CC) $(CFLAGS) bench_formula_test.c -o $(BENCH_TEST)

bench-test: $(BENCH_TEST)
	./$(BENCH_TEST)

$(CMD_PROCESSOR_TEST): $(CMD_PROCESSOR_TEST_DEPS)
	$(CC) $(CFLAGS) -I$(CMD_PROCESSOR_DIR) $(CMD_PROCESSOR_TEST_SRC) -o $(CMD_PROCESSOR_TEST) -pthread

test-cmd-processor: $(CMD_PROCESSOR_TEST)
	$(CMD_PROCESSOR_TEST_RUN)

$(ENGINE_CMD_PROCESSOR_TEST): $(ENGINE_CMD_PROCESSOR_TEST_DEPS)
	$(CC) $(CFLAGS) -I$(CMD_PROCESSOR_DIR) $(ENGINE_CMD_PROCESSOR_TEST_SRC) -o $(ENGINE_CMD_PROCESSOR_TEST) -pthread

test-engine-cmd-processor: $(ENGINE_CMD_PROCESSOR_TEST)
	$(ENGINE_CMD_PROCESSOR_TEST_RUN)

$(TCP_CMD_PROCESSOR_TEST): $(TCP_CMD_PROCESSOR_TEST_DEPS)
	$(CC) $(CFLAGS) $(TCP_CMD_PROCESSOR_TEST_CFLAGS) -I$(CMD_PROCESSOR_DIR) -I$(CJSON_DIR) $(TCP_CMD_PROCESSOR_TEST_SRC) -o $(TCP_CMD_PROCESSOR_TEST) -pthread

test-tcp-cmd-processor: $(TCP_CMD_PROCESSOR_TEST)
	$(TCP_CMD_PROCESSOR_TEST_RUN)

$(REPL_CMD_PROCESSOR_TEST): $(REPL_CMD_PROCESSOR_TEST_DEPS)
	$(CC) $(CFLAGS) -I$(CMD_PROCESSOR_DIR) $(REPL_CMD_PROCESSOR_TEST_SRC) -o $(REPL_CMD_PROCESSOR_TEST) -pthread

test-repl-cmd-processor: $(REPL_CMD_PROCESSOR_TEST)
	$(REPL_CMD_PROCESSOR_TEST_RUN)

$(CMD_PROCESSOR_SCALE_SCORE_TEST): $(CMD_PROCESSOR_SCALE_SCORE_TEST_DEPS)
	$(CC) $(CFLAGS) -I$(CMD_PROCESSOR_DIR) $(CMD_PROCESSOR_SCALE_SCORE_TEST_SRC) -o $(CMD_PROCESSOR_SCALE_SCORE_TEST) -pthread

test-cmd-processor-scale-score: $(CMD_PROCESSOR_SCALE_SCORE_TEST)
	$(CMD_PROCESSOR_SCALE_SCORE_TEST_RUN)

docker-build:
	docker compose build sqlprocessor

docker-test:
	docker compose run --rm test

docker-test-scale:
	docker compose run --rm test-scale

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

benchmark: $(STRESS_TARGET)
	./$(STRESS_TARGET) --benchmark 1000000

benchmark-jungle: $(STRESS_TARGET)
	./$(STRESS_TARGET) --benchmark-jungle 1000000

bench-smoke: build bench-tools
	./$(BENCH_RUNNER) --exec-spec $(BENCH_EXEC_SPEC) --profile smoke --seed 20260415 --repeat 3 --memtrack

bench-score: build bench-tools
ifeq ($(BENCH_SCORE_IN_TMP),1)
	tr -d '\r' < scripts/run_bench_score_tmp.sh | sh -s -- "$(CURDIR)" "$(BENCH_SCORE_UPDATE_ROWS)" "$(BENCH_SCORE_DELETE_ROWS)" "$(BENCH_EXEC_SPEC)"
else
	./$(BENCH_RUNNER) --exec-spec $(BENCH_EXEC_SPEC) --profile score --seed 20260415 --repeat 1 --update-rows $(BENCH_SCORE_UPDATE_ROWS) --delete-rows $(BENCH_SCORE_DELETE_ROWS) --memtrack
endif

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
	rm -f $(TARGET) $(TCP_SERVER_TARGET) $(STRESS_TARGET) $(BENCH_GEN) $(BENCH_RUNNER) $(BENCH_TEST) $(CMD_PROCESSOR_TEST) $(ENGINE_CMD_PROCESSOR_TEST) $(TCP_CMD_PROCESSOR_TEST) $(REPL_CMD_PROCESSOR_TEST) $(CMD_PROCESSOR_SCALE_SCORE_TEST)
