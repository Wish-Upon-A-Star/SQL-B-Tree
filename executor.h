#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "types.h"

void execute_insert(Statement *stmt);
void execute_select(Statement *stmt);
void execute_update(Statement *stmt);
void execute_delete(Statement *stmt);
void generate_jungle_dataset(int record_count, const char *filename);
void run_bplus_benchmark(int record_count);
void run_jungle_benchmark(int record_count);
void close_all_tables(void);
void set_executor_quiet(int quiet);

#endif
