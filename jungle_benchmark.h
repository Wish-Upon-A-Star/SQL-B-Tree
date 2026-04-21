#ifndef JUNGLE_BENCHMARK_H
#define JUNGLE_BENCHMARK_H

#include <stddef.h>

#define JUNGLE_BENCHMARK_CSV "jungle_benchmark_users.csv"
#define JUNGLE_BENCHMARK_TABLE "jungle_benchmark_users"
#define JUNGLE_BENCHMARK_HEADER \
    "id(PK),email(UK),phone(UK),name,track(NN),background,history,pretest,github,status,round\n"

int jungle_ensure_artifacts_absent(void);
void build_jungle_email(int id, char *buffer, size_t buffer_size);
void build_jungle_phone(int id, char *buffer, size_t buffer_size);
void build_jungle_name(int id, char *buffer, size_t buffer_size);
void build_jungle_row_data(int id, char *buffer, size_t buffer_size);
void generate_jungle_dataset(int record_count, const char *filename);

#endif
