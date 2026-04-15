CC ?= gcc
CFLAGS ?= -fdiagnostics-color=always -g
TARGET ?= sqlsprocessor
SRC = main.c
SRC_DEPS = main.c lexer.c parser.c bptree.c executor.c lexer.h parser.h bptree.h executor.h types.h
SQL ?= demo_bptree.sql
PYTHON ?= python
JUNGLE_DATASET ?= jungle_benchmark_users.csv
JUNGLE_RECORDS ?= 1000000

.PHONY: all build run demo-bptree demo-jungle scenario-jungle-regression scenario-jungle-range-and-replay scenario-jungle-update-constraints generate-jungle generate-jungle-sql benchmark benchmark-jungle clean

all: build

build: $(TARGET)

$(TARGET): $(SRC_DEPS)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

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

clean:
	rm -f $(TARGET)

benchmark: $(TARGET)
	./$(TARGET) --benchmark 1000000

benchmark-jungle: $(TARGET)
	./$(TARGET) --benchmark-jungle 1000000
