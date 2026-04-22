#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>
#if defined(_WIN32)
#include <windows.h>
#endif

#include "types.h"
#include "parser.h"
#include "executor.h"
#include "cmd_processor/cmd_processor.h"
#include "cmd_processor/cmd_processor_sync_bridge.h"
#include "cmd_processor/engine_cmd_processor.h"
#include "bench_memtrack.h"

static void configure_console_encoding(void);
static void copy_trimmed(char *dst, size_t dst_size, const char *start, const char *end);
static int starts_with_keyword(const char *text, const char *keyword);
static const char *skip_utf8_bom_and_space(const char *sql);
static void dispatch_statement(const Statement *stmt);
typedef enum {
    APP_MODE_EXEC_FILE,
    APP_MODE_GENERATE_JUNGLE
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
static int ensure_cmd_processor(void);
static void shutdown_cmd_processor(void);
static void next_request_id(char buffer[64]);

static CmdProcessor *g_cmd_processor = NULL;

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

static void next_request_id(char buffer[64]) {
    static uint64_t counter = 0;
    uint64_t now = (uint64_t)time(NULL);
    if (!buffer) return;
    counter++;
    snprintf(buffer, 64, "req-%llu-%llu",
             (unsigned long long)now,
             (unsigned long long)counter);
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
        case APP_MODE_EXEC_FILE:
        default:
            if (!ensure_cmd_processor()) return 1;
            if (execute_sql_file(config->filename) != 0) return 1;
            shutdown_cmd_processor();
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
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;
    CmdStatusCode set_status;
    const char *normalized_sql = skip_utf8_bom_and_space(sql);
    char request_id[64];

    if (*normalized_sql == '\0') return;
    if (!ensure_cmd_processor()) {
        printf("[오류] CmdProcessor 초기화에 실패했습니다.\n");
        return;
    }
    if (cmd_processor_acquire_request(g_cmd_processor, &request) != 0 || !request) {
        printf("[오류] 요청 버퍼를 확보하지 못했습니다.\n");
        return;
    }
    next_request_id(request_id);
    set_status = cmd_processor_set_sql_request(g_cmd_processor, request, request_id, normalized_sql);
    if (set_status != CMD_STATUS_OK) {
        if (cmd_processor_make_error_response(g_cmd_processor,
                                              request_id,
                                              set_status,
                                              "SQL request validation failed",
                                              &response) != 0) {
            printf("[오류] 요청 검증 실패를 응답으로 변환하지 못했습니다.\n");
            cmd_processor_release_request(g_cmd_processor, request);
                return;
        }
    } else {
        if (cmd_processor_submit_sync(g_cmd_processor, request, &response) != 0) {
            printf("[오류] SQL 실행 중 내부 오류가 발생했습니다.\n");
            cmd_processor_release_request(g_cmd_processor, request);
            return;
        }
    }

    if (set_status == CMD_STATUS_OK && !response) {
        printf("[오류] SQL 실행 중 내부 오류가 발생했습니다.\n");
        cmd_processor_release_request(g_cmd_processor, request);
        return;
    }

    if (response->body && response->body[0] != '\0') {
        printf("%s\n", response->body);
    } else if (response->error_message && response->error_message[0] != '\0') {
        printf("[오류] %s\n", response->error_message);
    } else {
        printf("[%s]\n", cmd_status_to_string(response->status));
    }

    cmd_processor_release_response(g_cmd_processor, response);
    cmd_processor_release_request(g_cmd_processor, request);
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

static int ensure_cmd_processor(void) {
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;

    if (g_cmd_processor) return 1;
    memset(&context, 0, sizeof(context));
    context.name = "sqlsprocessor_engine";
    context.max_sql_len = MAX_SQL_LEN - 1;
    context.request_buffer_count = 0;
    context.response_body_capacity = 4096;

    memset(&options, 0, sizeof(options));
    options.worker_count = 2;
    options.shard_count = 2;
    options.queue_capacity_per_shard = 64;
    options.planner_cache_capacity = 128;

    return engine_cmd_processor_create(&context, &options, &g_cmd_processor) == 0;
}

static void shutdown_cmd_processor(void) {
    if (!g_cmd_processor) return;
    cmd_processor_shutdown(g_cmd_processor);
    g_cmd_processor = NULL;
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
