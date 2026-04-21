#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#if defined(_WIN32)
#include <windows.h>
#endif

#include "types.h"
#include "parser.h"
#include "executor.h"

#if defined(BENCH_MEMTRACK)
typedef struct BenchAllocHeader {
    size_t requested;
} BenchAllocHeader;

static size_t g_bench_live_requested_bytes = 0;
static size_t g_bench_peak_requested_bytes = 0;

static void bench_memtrack_update_peak(void) {
    if (g_bench_live_requested_bytes > g_bench_peak_requested_bytes) {
        g_bench_peak_requested_bytes = g_bench_live_requested_bytes;
    }
}

static void *bench_malloc(size_t size) {
    BenchAllocHeader *header;
    size_t total;

    if (size > ((size_t)-1) - sizeof(BenchAllocHeader)) return NULL;
    total = size + sizeof(BenchAllocHeader);
    header = (BenchAllocHeader *)(malloc)(total);
    if (!header) return NULL;
    header->requested = size;
    g_bench_live_requested_bytes += size;
    bench_memtrack_update_peak();
    return (void *)(header + 1);
}

static void *bench_calloc(size_t nmemb, size_t size) {
    size_t requested;
    void *ptr;

    if (nmemb != 0 && size > ((size_t)-1) / nmemb) return NULL;
    requested = nmemb * size;
    ptr = bench_malloc(requested);
    if (ptr) memset(ptr, 0, requested);
    return ptr;
}

static void *bench_realloc(void *ptr, size_t size) {
    BenchAllocHeader *old_header;
    BenchAllocHeader *new_header;
    size_t old_size;
    size_t total;

    if (!ptr) return bench_malloc(size);
    if (size == 0) {
        (free)(((BenchAllocHeader *)ptr) - 1);
        return NULL;
    }

    old_header = ((BenchAllocHeader *)ptr) - 1;
    old_size = old_header->requested;
    if (size > ((size_t)-1) - sizeof(BenchAllocHeader)) return NULL;
    total = size + sizeof(BenchAllocHeader);
    new_header = (BenchAllocHeader *)(realloc)(old_header, total);
    if (!new_header) return NULL;

    if (g_bench_live_requested_bytes >= old_size) g_bench_live_requested_bytes -= old_size;
    else g_bench_live_requested_bytes = 0;
    g_bench_live_requested_bytes += size;
    new_header->requested = size;
    bench_memtrack_update_peak();
    return (void *)(new_header + 1);
}

static void bench_free(void *ptr) {
    BenchAllocHeader *header;
    if (!ptr) return;
    header = ((BenchAllocHeader *)ptr) - 1;
    if (g_bench_live_requested_bytes >= header->requested) {
        g_bench_live_requested_bytes -= header->requested;
    } else {
        g_bench_live_requested_bytes = 0;
    }
    (free)(header);
}

static void bench_memtrack_report(void) {
    const char *enabled = getenv("BENCH_MEMTRACK_REPORT");
    if (enabled && enabled[0] != '\0' && enabled[0] != '0') {
        printf("[memtrack] peak_heap_requested_bytes=%zu current_live_requested_bytes=%zu\n",
               g_bench_peak_requested_bytes, g_bench_live_requested_bytes);
    }
}

#define malloc(sz) bench_malloc(sz)
#define calloc(n, sz) bench_calloc(n, sz)
#define realloc(ptr, sz) bench_realloc(ptr, sz)
#define free(ptr) bench_free(ptr)
#endif

static void configure_console_encoding(void);
static void copy_trimmed(char *dst, size_t dst_size, const char *start, const char *end);
static int starts_with_keyword(const char *text, const char *keyword);
static const char *skip_utf8_bom_and_space(const char *sql);
static void dispatch_statement(const Statement *stmt);
typedef enum {
    APP_MODE_EXEC_FILE,
    APP_MODE_GENERATE_JUNGLE,
    APP_MODE_BENCHMARK,
    APP_MODE_BENCHMARK_JUNGLE
} AppMode;

typedef struct {
    AppMode mode;
    int quiet;
    int count;
    const char *dataset_output;
    char filename[256];
} AppConfig;

static void init_app_config(AppConfig *config);
static int consume_flag(const char *arg, const char *flag);
static int parse_count_arg(int argc, char *argv[], int index, int fallback);
static int read_input_filename(AppConfig *config);
static int parse_app_config(int argc, char *argv[], AppConfig *config);
static int run_app(const AppConfig *config);
static int parse_fast_insert_sql(const char *sql);
static int parse_fast_update_sql(const char *sql);
static int parse_fast_delete_sql(const char *sql);
static int try_execute_fast_sql(const char *sql);
static void execute_sql_text(const char *sql);
static int flush_sql_buffer(char *sql_buffer, int *length, int *too_long);
static int execute_sql_file(const char *filename);

static void configure_console_encoding(void) {
#if defined(_WIN32)
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
#endif
}

static void copy_trimmed(char *dst, size_t dst_size, const char *start, const char *end) {
    size_t len;

    if (!dst || dst_size == 0 || !start || !end || end < start) return;
    while (start < end && isspace((unsigned char)*start)) start++;
    while (end > start && isspace((unsigned char)*(end - 1))) end--;
    if (end > start && *start == '\'' && *(end - 1) == '\'') {
        start++;
        end--;
    }
    len = (size_t)(end - start);
    if (len >= dst_size) len = dst_size - 1;
    memcpy(dst, start, len);
    dst[len] = '\0';
}

static int starts_with_keyword(const char *text, const char *keyword) {
    size_t length = strlen(keyword);

    return strncmp(text, keyword, length) == 0 &&
           (text[length] == '\0' || isspace((unsigned char)text[length]));
}

static const char *skip_utf8_bom_and_space(const char *sql) {
    if ((unsigned char)sql[0] == 0xEF &&
        (unsigned char)sql[1] == 0xBB &&
        (unsigned char)sql[2] == 0xBF) {
        sql += 3;
    }

    while (isspace((unsigned char)*sql)) sql++;
    return sql;
}

static void dispatch_statement(const Statement *stmt) {
    switch (stmt->type) {
        case STMT_INSERT:
            execute_insert((Statement *)stmt);
            return;
        case STMT_SELECT:
            execute_select((Statement *)stmt);
            return;
        case STMT_DELETE:
            execute_delete((Statement *)stmt);
            return;
        case STMT_UPDATE:
            execute_update((Statement *)stmt);
            return;
        default:
            return;
    }
}

static void init_app_config(AppConfig *config) {
    memset(config, 0, sizeof(*config));
    config->mode = APP_MODE_EXEC_FILE;
    config->count = 1000000;
}

static int consume_flag(const char *arg, const char *flag) {
    return arg && strcmp(arg, flag) == 0;
}

static int parse_count_arg(int argc, char *argv[], int index, int fallback) {
    return (index < argc) ? atoi(argv[index]) : fallback;
}

static int read_input_filename(AppConfig *config) {
    printf("입력 SQL 파일 경로: ");
    return scanf("%255s", config->filename) == 1;
}

static int parse_app_config(int argc, char *argv[], AppConfig *config) {
    int argi = 1;

    init_app_config(config);

    if (argi < argc && consume_flag(argv[argi], "--quiet")) {
        config->quiet = 1;
        argi++;
    }

    if (argi < argc && consume_flag(argv[argi], "--generate-jungle")) {
        config->mode = APP_MODE_GENERATE_JUNGLE;
        config->count = parse_count_arg(argc, argv, argi + 1, 1000000);
        config->dataset_output = (argi + 2 < argc) ? argv[argi + 2] : NULL;
        return 1;
    }

    if (argi < argc && consume_flag(argv[argi], "--benchmark")) {
        config->mode = APP_MODE_BENCHMARK;
        config->count = parse_count_arg(argc, argv, argi + 1, 1000000);
        return 1;
    }

    if (argi < argc && consume_flag(argv[argi], "--benchmark-jungle")) {
        config->mode = APP_MODE_BENCHMARK_JUNGLE;
        config->count = parse_count_arg(argc, argv, argi + 1, 1000000);
        return 1;
    }

    if (argi < argc) {
        strncpy(config->filename, argv[argi], sizeof(config->filename) - 1);
        config->filename[sizeof(config->filename) - 1] = '\0';
        return 1;
    }

    return read_input_filename(config);
}

static int run_app(const AppConfig *config) {
    if (config->quiet) {
        set_executor_quiet(1);
    }

    switch (config->mode) {
        case APP_MODE_GENERATE_JUNGLE:
            generate_jungle_dataset(config->count, config->dataset_output);
            return 0;
        case APP_MODE_BENCHMARK:
            run_bplus_benchmark(config->count);
            return 0;
        case APP_MODE_BENCHMARK_JUNGLE:
            run_jungle_benchmark(config->count);
            return 0;
        case APP_MODE_EXEC_FILE:
        default:
            if (execute_sql_file(config->filename) != 0) return 1;
            close_all_tables();
            return 0;
    }
}

static int parse_fast_insert_sql(const char *sql) {
    const char *p;
    const char *values_kw;
    const char *close_paren;
    char table[256];
    char values[MAX_SQL_LEN];

    if (!starts_with_keyword(sql, "INSERT")) return 0;

    p = strstr(sql, "INTO");
    if (!p) return 0;
    p += 4;

    while (isspace((unsigned char)*p)) p++;
    values_kw = strstr(p, "VALUES");
    if (!values_kw) return 0;

    copy_trimmed(table, sizeof(table), p, values_kw);
    p = strchr(values_kw, '(');
    close_paren = strrchr(values_kw, ')');
    if (!p || !close_paren || close_paren <= p) return 0;

    copy_trimmed(values, sizeof(values), p + 1, close_paren);
    return execute_insert_values_fast(table, values);
}

static int parse_fast_update_sql(const char *sql) {
    const char *cursor;
    const char *set_kw;
    const char *where_kw;
    const char *eq;
    char table[256];
    char column[50];
    char value[256];
    char id_value[64];

    if (!starts_with_keyword(sql, "UPDATE")) return 0;

    cursor = sql + 6;
    while (isspace((unsigned char)*cursor)) cursor++;

    set_kw = strstr(cursor, "SET");
    where_kw = strstr(cursor, "WHERE");
    if (!set_kw || !where_kw || where_kw <= set_kw) return 0;

    copy_trimmed(table, sizeof(table), cursor, set_kw);
    cursor = set_kw + 3;
    eq = strchr(cursor, '=');
    if (!eq || eq > where_kw) return 0;

    copy_trimmed(column, sizeof(column), cursor, eq);
    copy_trimmed(value, sizeof(value), eq + 1, where_kw);

    cursor = where_kw + 5;
    while (isspace((unsigned char)*cursor)) cursor++;
    if (strncmp(cursor, "id", 2) != 0 ||
        !(cursor[2] == '\0' || isspace((unsigned char)cursor[2]) || cursor[2] == '=')) {
        return 0;
    }

    eq = strchr(cursor, '=');
    if (!eq) return 0;

    copy_trimmed(id_value, sizeof(id_value), eq + 1, sql + strlen(sql));
    return execute_update_id_fast(table, column, value, id_value);
}

static int parse_fast_delete_sql(const char *sql) {
    const char *from_kw;
    const char *where_kw;
    const char *cursor;
    const char *eq;
    char table[256];
    char id_value[64];

    if (!starts_with_keyword(sql, "DELETE")) return 0;

    from_kw = strstr(sql, "FROM");
    where_kw = strstr(sql, "WHERE");
    if (!from_kw || !where_kw || where_kw <= from_kw) return 0;

    cursor = from_kw + 4;
    copy_trimmed(table, sizeof(table), cursor, where_kw);

    cursor = where_kw + 5;
    while (isspace((unsigned char)*cursor)) cursor++;
    if (strncmp(cursor, "id", 2) != 0 ||
        !(cursor[2] == '\0' || isspace((unsigned char)cursor[2]) || cursor[2] == '=')) {
        return 0;
    }

    eq = strchr(cursor, '=');
    if (!eq) return 0;

    copy_trimmed(id_value, sizeof(id_value), eq + 1, sql + strlen(sql));
    return execute_delete_id_fast(table, id_value);
}

static int try_execute_fast_sql(const char *sql) {
    if (parse_fast_insert_sql(sql)) return 1;
    if (parse_fast_update_sql(sql)) return 1;
    if (parse_fast_delete_sql(sql)) return 1;

    return 0;
}

static void execute_sql_text(const char *sql) {
    Statement stmt;
    const char *normalized_sql = skip_utf8_bom_and_space(sql);

    if (*normalized_sql == '\0') return;
    if (try_execute_fast_sql(normalized_sql)) return;

    if (!parse_statement(normalized_sql, &stmt)) {
        printf("[오류] 잘못된 SQL 문장입니다: %s\n", normalized_sql);
        return;
    }

    dispatch_statement(&stmt);
}

static int flush_sql_buffer(char *sql_buffer, int *length, int *too_long) {
    sql_buffer[*length] = '\0';

    if (*too_long) {
        printf("[오류] SQL 문장이 너무 깁니다. 최대 길이=%d\n", MAX_SQL_LEN - 1);
        *length = 0;
        *too_long = 0;
        return 0;
    }

    execute_sql_text(sql_buffer);
    *length = 0;
    *too_long = 0;
    return 1;
}

static int execute_sql_file(const char *filename) {
    FILE *file = fopen(filename, "r");
    char sql_buffer[MAX_SQL_LEN];
    char line[MAX_SQL_LEN];
    int buffer_length = 0;
    int in_quote = 0;
    int too_long = 0;

    if (!file) {
        printf("[오류] '%s' 파일을 열 수 없습니다.\n", filename);
        return 1;
    }

    while (fgets(line, sizeof(line), file)) {
        char *cursor = line;

        if (!in_quote) {
            while (isspace((unsigned char)*cursor)) cursor++;
            if (cursor[0] == '-' && cursor[1] == '-') continue;
        }

        for (; *cursor; cursor++) {
            if (*cursor == '\'') in_quote = !in_quote;

            if (*cursor == ';' && !in_quote) {
                flush_sql_buffer(sql_buffer, &buffer_length, &too_long);
                continue;
            }

            if (buffer_length < MAX_SQL_LEN - 1) {
                sql_buffer[buffer_length++] = *cursor;
            } else {
                too_long = 1;
            }
        }
    }

    if (buffer_length > 0) {
        flush_sql_buffer(sql_buffer, &buffer_length, &too_long);
    }

    fclose(file);
    return 0;
}

/* SQL 파일에서 ';'로 구분되는 SQL 문장을 순서대로 실행합니다. */
int main(int argc, char *argv[]) {
    AppConfig config;

    configure_console_encoding();
#if defined(BENCH_MEMTRACK)
    atexit(bench_memtrack_report);
#endif

    if (!parse_app_config(argc, argv, &config)) return 1;
    return run_app(&config);
}

/*
 * 일부 IDE/에디터는 "현재 파일만 컴파일" 방식으로 main.c 하나만 빌드합니다.
 * 그 빌드 경로는 별도 번들 파일이 담당하고, main.c는 엔트리포인트 흐름만 남깁니다.
 */
#include "sqlsprocessor_bundle.h"
