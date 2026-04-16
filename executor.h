#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "types.h"

void execute_insert(Statement *stmt);
void execute_select(Statement *stmt);
void execute_update(Statement *stmt);
void execute_delete(Statement *stmt);
int execute_insert_values_fast(const char *table_name, const char *values_csv);
int execute_update_id_fast(const char *table_name, const char *set_col, const char *set_value, const char *id_value);
int execute_delete_id_fast(const char *table_name, const char *id_value);
void generate_jungle_dataset(int record_count, const char *filename);
void run_bplus_benchmark(int record_count);
void run_jungle_benchmark(int record_count);
void close_all_tables(void);
void set_executor_quiet(int quiet);

#endif
