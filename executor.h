#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "types.h"

typedef struct {
    int matched_rows;
    int affected_rows;
    long generated_id;
    int select_column_count;
    int select_column_indices[MAX_COLS];
    char select_columns[MAX_COLS][50];
    char **select_rows;
    int select_row_count;
    int select_row_capacity;
} ExecutorResult;

void executor_result_init(ExecutorResult *result);
void executor_result_free(ExecutorResult *result);
void execute_insert(Statement *stmt);
void execute_select(Statement *stmt);
void execute_update(Statement *stmt);
void execute_delete(Statement *stmt);
void parse_csv_row(const char *row, char *fields[MAX_COLS], char *buffer);
int executor_execute_statement_with_result(Statement *stmt, ExecutorResult *result);
int executor_execute_statement(Statement *stmt, int *matched_rows, int *affected_rows, long *generated_id);
void generate_jungle_dataset(int record_count, const char *filename);
void run_bplus_benchmark(int record_count);
void run_jungle_benchmark(int record_count);
void close_all_tables(void);
void set_executor_quiet(int quiet);
int executor_runtime_init(void);
void executor_runtime_shutdown(void);

#endif
