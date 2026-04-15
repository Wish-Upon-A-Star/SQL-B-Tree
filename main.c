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

static void configure_console_encoding(void) {
#if defined(_WIN32)
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
#endif
}

static int try_execute_insert_fast(const char *sql);

static void execute_sql_text(const char *sql) {
    Statement stmt;
    const char *s = sql;

    if ((unsigned char)s[0] == 0xEF &&
        (unsigned char)s[1] == 0xBB &&
        (unsigned char)s[2] == 0xBF) {
        s += 3;
    }
    while (isspace((unsigned char)*s)) s++;
    if (*s == '\0') return;

    if (try_execute_insert_fast(s)) {
        return;
    }

    if (parse_statement(s, &stmt)) {
        if (stmt.type == STMT_INSERT) execute_insert(&stmt);
        else if (stmt.type == STMT_SELECT) execute_select(&stmt);
        else if (stmt.type == STMT_DELETE) execute_delete(&stmt);
        else if (stmt.type == STMT_UPDATE) execute_update(&stmt);
    } else {
        printf("[오류] 잘못된 SQL 문장입니다: %s\n", s);
    }
}

static int keyword_match_ci(const char *s, const char *kw) {
    while (*kw) {
        if (tolower((unsigned char)*s) != tolower((unsigned char)*kw)) return 0;
        s++;
        kw++;
    }
    return 1;
}

static const char *skip_sql_spaces(const char *s) {
    while (isspace((unsigned char)*s)) s++;
    return s;
}

static int try_execute_insert_fast(const char *sql) {
    Statement stmt;
    const char *p;
    const char *table_start;
    const char *values_start;
    const char *row_start;
    const char *row_end = NULL;
    int q = 0;
    size_t table_len;
    size_t row_len;

    p = skip_sql_spaces(sql);
    if (!keyword_match_ci(p, "insert")) return 0;
    p = skip_sql_spaces(p + 6);
    if (!keyword_match_ci(p, "into")) return 0;
    p = skip_sql_spaces(p + 4);

    table_start = p;
    while (*p && !isspace((unsigned char)*p) && *p != '(') p++;
    table_len = (size_t)(p - table_start);
    if (table_len == 0 || table_len >= sizeof(stmt.table_name)) return 0;

    p = skip_sql_spaces(p);
    if (!keyword_match_ci(p, "values")) return 0;
    p = skip_sql_spaces(p + 6);
    if (*p != '(') return 0;

    row_start = p + 1;
    for (values_start = row_start; *values_start; values_start++) {
        if (*values_start == '\'') q = !q;
        if (*values_start == ')' && !q) {
            row_end = values_start;
            break;
        }
    }
    if (!row_end) return 0;
    p = skip_sql_spaces(row_end + 1);
    if (*p != '\0') return 0;

    row_len = (size_t)(row_end - row_start);
    if (row_len >= sizeof(stmt.row_data)) return 0;

    memset(&stmt, 0, sizeof(stmt));
    stmt.type = STMT_INSERT;
    memcpy(stmt.table_name, table_start, table_len);
    stmt.table_name[table_len] = '\0';
    memcpy(stmt.row_data, row_start, row_len);
    stmt.row_data[row_len] = '\0';
    execute_insert(&stmt);
    return 1;
}

static int execute_sql_line_fast(char *line) {
    char *s = line;
    int q = 0;
    int semicolon_count = 0;
    char *semicolon = NULL;

    while (isspace((unsigned char)*s)) s++;
    if (*s == '\0' || (*s == '-' && s[1] == '-')) return 1;

    for (char *p = s; *p; p++) {
        if (*p == '\'') q = !q;
        if (*p == ';' && !q) {
            semicolon_count++;
            semicolon = p;
        }
    }
    if (semicolon_count != 1 || !semicolon) return 0;
    for (char *p = semicolon + 1; *p; p++) {
        if (!isspace((unsigned char)*p)) return 0;
    }
    *semicolon = '\0';
    execute_sql_text(s);
    return 1;
}

/* SQL 파일에서 ';'로 구분되는 SQL 문장을 순서대로 실행합니다. */
int main(int argc, char *argv[]) {
    configure_console_encoding();
#if defined(BENCH_MEMTRACK)
    atexit(bench_memtrack_report);
#endif

    char filename[256];
    int argi = 1;

    if (argi < argc && strcmp(argv[argi], "--quiet") == 0) {
        set_executor_quiet(1);
        argi++;
    }

    if (argi < argc && strcmp(argv[argi], "--generate-jungle") == 0) {
        int count = (argi + 1 < argc) ? atoi(argv[argi + 1]) : 1000000;
        const char *output = (argi + 2 < argc) ? argv[argi + 2] : NULL;
        generate_jungle_dataset(count, output);
        return 0;
    }

    if (argi < argc && strcmp(argv[argi], "--benchmark") == 0) {
        int count = (argi + 1 < argc) ? atoi(argv[argi + 1]) : 1000000;
        run_bplus_benchmark(count);
        close_all_tables();
        return 0;
    }
    if (argi < argc && strcmp(argv[argi], "--benchmark-jungle") == 0) {
        int count = (argi + 1 < argc) ? atoi(argv[argi + 1]) : 1000000;
        run_jungle_benchmark(count);
        return 0;
    }

    if (argi < argc) {
        strncpy(filename, argv[argi], 255);
        filename[255] = '\0';
    } else {
        printf("입력 SQL 파일 경로: ");
        if (scanf("%255s", filename) != 1) return 1;
    }

    FILE *f = fopen(filename, "r");
    if (!f) {
        printf("[오류] '%s' 파일을 열 수 없습니다.\n", filename);
        return 1;
    }

    char buf[MAX_SQL_LEN];
    char line[MAX_SQL_LEN];
    int idx = 0;
    int q = 0;
    int too_long = 0;

    while (fgets(line, sizeof(line), f)) {
        char *p;

        if (idx == 0 && execute_sql_line_fast(line)) {
            continue;
        }

        p = line;
        while (*p) {
            int ch = (unsigned char)*p++;
            if (ch == '-' && !q) {
                if (*p == '-') {
                    break;
                }
            }

            if (ch == '\'') q = !q;

            if (ch == ';' && !q) {
                buf[idx] = '\0';
                if (too_long) {
                    printf("[오류] SQL 문장이 너무 깁니다. 최대 길이=%d\n", MAX_SQL_LEN - 1);
                } else {
                    execute_sql_text(buf);
                }
                idx = 0;
                too_long = 0;
            } else if (idx < MAX_SQL_LEN - 1) {
                buf[idx++] = (char)ch;
            } else {
                too_long = 1;
            }
        }
    }

    if (idx > 0) {
        buf[idx] = '\0';
        if (too_long) {
            printf("[오류] SQL 문장이 너무 깁니다. 최대 길이=%d\n", MAX_SQL_LEN - 1);
        } else {
            execute_sql_text(buf);
        }
    }

    fclose(f);
    close_all_tables();

    return 0;
}

/*
 * 일부 IDE/에디터는 "현재 파일만 컴파일" 방식으로 main.c 하나만 빌드합니다.
 * 그 경우에도 바로 실행되도록 구현 파일을 여기서 함께 포함합니다.
 */
#include "lexer.c"
#include "parser.c"
#include "bptree.c"
#include "executor.c"
