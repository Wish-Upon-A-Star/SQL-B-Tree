#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#if defined(_WIN32)
#include <windows.h>
#endif

#include "executor.h"
#include "bptree.h"

#define DELTA_LINE_SIZE (RECORD_SIZE + 128)
#define TABLE_FILE_BUFFER_SIZE (1024 * 1024)

TableCache open_tables[MAX_TABLES];
int open_table_count = 0;
static unsigned long long g_table_access_seq = 0;
static int g_executor_quiet = 0;
static const char *const JUNGLE_BENCHMARK_CSV = "jungle_benchmark_users.csv";
static const char *const JUNGLE_BENCHMARK_TABLE = "jungle_benchmark_users";
static const char *const JUNGLE_BENCHMARK_HEADER =
    "id(PK),email(UK),phone(UK),name,track(NN),background,history,pretest,github,status,round\n";

#define INFO_PRINTF(...) do { if (!g_executor_quiet) printf(__VA_ARGS__); } while (0)

void set_executor_quiet(int quiet) {
    g_executor_quiet = quiet ? 1 : 0;
}

void parse_csv_row(const char *row, char *fields[MAX_COLS], char *buffer);
static void normalize_value(const char *src, char *dest, size_t dest_size);
static int rebuild_id_index(TableCache *tc);
static int rebuild_uk_indexes(TableCache *tc);
static int index_record_uks(TableCache *tc, int row_index);
static int index_record_uks_from_row(TableCache *tc, const char *row, int row_index);
static int get_uk_slot(TableCache *tc, int col_idx);
static int ensure_uk_indexes(TableCache *tc);
static void rollback_updated_records(TableCache *tc, char **old_records);
static int remove_record_indexes(TableCache *tc, const char *row);
static int restore_record_indexes(TableCache *tc, int slot_id);
static int append_csv_field(char *row, size_t row_size, size_t *offset, const char *value, int is_last);
static int parse_long_value(const char *value, long *out);
static int slot_is_active(TableCache *tc, int slot_id);
static char *slot_row(TableCache *tc, int slot_id);
static char *slot_row_scan(TableCache *tc, int slot_id, int *owned);
static char *read_row_from_page_cache(TableCache *tc, long offset);
static void clear_page_cache(TableCache *tc);
static int evict_row_cache_if_needed(TableCache *tc);
static int assign_slot_row(TableCache *tc, int slot_id, const char *row,
                           RowStoreType store_type, long offset, int cache_row);
static int table_file_has_value(TableCache *tc, int col_idx, const char *value);
static int for_each_file_row_from(TableCache *tc, long start_offset,
                                  int (*visitor)(TableCache *, const char *, void *),
                                  void *ctx);
static int replay_delta_log(TableCache *tc);
static int clear_delta_log(TableCache *tc);
static int maybe_compact_delta_log(TableCache *tc);
static int close_delta_batch(TableCache *tc);
static int load_index_snapshot(TableCache *tc);
static int save_index_snapshot(TableCache *tc);
static void remove_index_snapshot(TableCache *tc);
static int load_table_contents(TableCache *tc, const char *name, FILE *f);
static int execute_update_single_row(TableCache *tc, Statement *stmt, int where_idx,
                                     const WhereCondition *lookup_cond, int set_idx, const char *set_value, int uses_pk_lookup,
                                     int uses_uk_lookup, int rebuild_uk_needed);
static int row_fields_match_statement(TableCache *tc, Statement *stmt, char *fields[MAX_COLS]);
int rewrite_file(TableCache *tc);

static char *dup_string(const char *src) {
    size_t len = strlen(src) + 1;
    char *copy = (char *)malloc(len);
    if (!copy) return NULL;
    memcpy(copy, src, len);
    return copy;
}

static int compare_bplus_pair(const void *a, const void *b) {
    const BPlusPair *pa = (const BPlusPair *)a;
    const BPlusPair *pb = (const BPlusPair *)b;
    return (pa->key > pb->key) - (pa->key < pb->key);
}

static int compare_bplus_string_pair(const void *a, const void *b) {
    const BPlusStringPair *pa = (const BPlusStringPair *)a;
    const BPlusStringPair *pb = (const BPlusStringPair *)b;
    return strcmp(pa->key, pb->key);
}

static int path_exists(const char *filename) {
    struct stat st;

    return filename && stat(filename, &st) == 0;
}

static int clamp_record_count(int record_count, int minimum) {
    if (record_count < minimum) record_count = minimum;
    if (record_count > MAX_RECORDS) {
        printf("[notice] record count capped at MAX_RECORDS=%d.\n", MAX_RECORDS);
        record_count = MAX_RECORDS;
    }
    return record_count;
}

struct UniqueIndex {
    BPlusStringTree *tree;
    int col_idx;
};

static UniqueIndex *unique_index_create(int col_idx) {
    UniqueIndex *index = (UniqueIndex *)calloc(1, sizeof(UniqueIndex));
    if (!index) return NULL;
    index->col_idx = col_idx;
    index->tree = bptree_string_create();
    if (!index->tree) {
        free(index);
        return NULL;
    }
    return index;
}

static void unique_index_destroy(UniqueIndex *index) {
    if (!index) return;
    bptree_string_destroy(index->tree);
    free(index);
}

static int unique_index_find(TableCache *tc, UniqueIndex *index, const char *key, int *row_index) {
    int found_row;

    if (!index || !key || strlen(key) == 0) return 0;
    if (!bptree_string_search(index->tree, key, &found_row)) return 0;
    if (tc && !slot_is_active(tc, found_row)) return 0;
    if (row_index) *row_index = found_row;
    return 1;
}

static int find_uk_row(TableCache *tc, int col_idx, const char *value, int *row_index) {
    char key[RECORD_SIZE];
    int uk_slot;

    if (!tc || col_idx < 0 || !value) return 0;
    uk_slot = get_uk_slot(tc, col_idx);
    if (uk_slot == -1 || !ensure_uk_indexes(tc)) return 0;
    normalize_value(value, key, sizeof(key));
    if (strlen(key) == 0) return 0;
    return unique_index_find(tc, tc->uk_indexes[uk_slot], key, row_index);
}

static int find_pk_row(TableCache *tc, const char *value, int *row_index) {
    long key;
    int found_row;

    if (!tc || tc->pk_idx == -1 || !value) return 0;
    if (!parse_long_value(value, &key)) return 0;
    if (!bptree_search(tc->id_index, key, &found_row)) return 0;
    if (!slot_is_active(tc, found_row)) return 0;
    if (row_index) *row_index = found_row;
    return 1;
}

static const char *jungle_track_for_id(int id) {
    static const char *const tracks[] = {
        "sw_ai_lab", "game_lab", "game_tech_lab"
    };
    return tracks[(id - 1) % 3];
}

static const char *jungle_background_for_id(int id) {
    int bucket = (id - 1) % 100;

    if (bucket < 62) return "student";
    if (bucket < 74) return "newgrad";
    if (bucket < 86) return "incumbent";
    if (bucket < 95) return "switcher";
    return "selftaught";
}

static int jungle_pretest_for_id(int id) {
    long long mixed = (long long)id * 73LL + (long long)((id - 1) % 97) * 19LL + 17LL;
    return 35 + (int)(mixed % 66LL);
}

static void build_jungle_history(int id, const char *background, char *buffer, size_t buffer_size) {
    static const char *const majors[] = {
        "cs", "software", "ai", "game", "math", "physics",
        "stats", "design", "ee", "business", "english", "biology"
    };
    static const char *const incumbent_roles[] = {
        "backend", "frontend", "data", "infra", "qa", "game_client", "game_server"
    };
    static const char *const switcher_roles[] = {
        "designer", "teacher", "marketer", "pm", "sales", "mechanical", "accounting"
    };
    static const char *const selftaught_routes[] = {
        "selftaught", "bootcamp", "indie", "academy"
    };
    int major_idx = ((id - 1) / 3) % (int)(sizeof(majors) / sizeof(majors[0]));

    if (strcmp(background, "student") == 0) {
        int grade = ((id - 1) / 7) % 4 + 1;
        snprintf(buffer, buffer_size, "major_%s_grade_%d", majors[major_idx], grade);
        return;
    }

    if (strcmp(background, "newgrad") == 0) {
        snprintf(buffer, buffer_size, "major_%s_graduate", majors[major_idx]);
        return;
    }

    if (strcmp(background, "incumbent") == 0) {
        int role_idx = ((id - 1) / 5) % (int)(sizeof(incumbent_roles) / sizeof(incumbent_roles[0]));
        int years = ((id - 1) / 11) % 6 + 1;
        snprintf(buffer, buffer_size, "%s_%dy", incumbent_roles[role_idx], years);
        return;
    }

    if (strcmp(background, "switcher") == 0) {
        int role_idx = ((id - 1) / 9) % (int)(sizeof(switcher_roles) / sizeof(switcher_roles[0]));
        int years = ((id - 1) / 13) % 8 + 1;
        snprintf(buffer, buffer_size, "%s_%dy", switcher_roles[role_idx], years);
        return;
    }

    {
        int route_idx = ((id - 1) / 17) % (int)(sizeof(selftaught_routes) / sizeof(selftaught_routes[0]));
        int months = ((((id - 1) / 19) % 9) + 1) * 6;
        snprintf(buffer, buffer_size, "%s_%dm", selftaught_routes[route_idx], months);
    }
}

static void build_jungle_github(int id, const char *background, char *buffer, size_t buffer_size) {
    int bucket = (int)(((long long)id * 29LL + 7LL) % 100LL);

    if ((strcmp(background, "student") == 0 && bucket < 18) ||
        (strcmp(background, "newgrad") == 0 && bucket < 8) ||
        (strcmp(background, "switcher") == 0 && bucket < 10) ||
        (strcmp(background, "selftaught") == 0 && bucket < 6)) {
        snprintf(buffer, buffer_size, "none");
        return;
    }

    snprintf(buffer, buffer_size, "gh_%07d", id);
}

static const char *jungle_status_for_id(int id, int pretest) {
    if (id % 113 == 0) return "withdrawn";
    if (pretest >= 98) return "final_pass";
    if (pretest >= 90) return "final_wait";
    if (pretest >= 80) return "interview_wait";
    if (pretest >= 65) return "pretest_pass";
    if (pretest >= 50) return "submitted";
    return "rejected";
}

static void build_jungle_email(int id, char *buffer, size_t buffer_size) {
    snprintf(buffer, buffer_size, "jungle%07d@apply.kr", id);
}

static void build_jungle_phone(int id, char *buffer, size_t buffer_size) {
    snprintf(buffer, buffer_size, "010-%04d-%04d", id / 10000, id % 10000);
}

static void build_jungle_name(int id, char *buffer, size_t buffer_size) {
    static const char *const surnames[] = {
        "김", "이", "박", "최", "정", "강", "조", "윤", "장", "임",
        "한", "오", "서", "신", "권", "황", "안", "송", "전", "홍"
    };
    static const char *const first_syllables[] = {
        "민", "서", "지", "도", "하", "수", "예", "시",
        "현", "준", "유", "주", "다", "윤", "태", "선",
        "건", "채", "승", "정", "호", "은", "재", "가",
        "나", "라", "마", "소", "아", "연", "원", "진",
        "혜", "규", "한", "슬", "보", "새", "강", "온",
        "루", "단", "류", "리", "해", "별", "솔", "율"
    };
    static const char *const second_syllables[] = {
        "준", "연", "후", "윤", "린", "민", "아", "우",
        "서", "진", "수", "영", "원", "호", "혜", "현",
        "지", "인", "온", "별", "율", "나", "리", "빈",
        "솔", "람", "훈", "석", "비", "담", "새", "균",
        "채", "한", "태", "경", "슬", "주", "재", "강",
        "선", "도", "희", "휘", "록", "봄", "혁", "화"
    };
    int surname_count = (int)(sizeof(surnames) / sizeof(surnames[0]));
    int first_count = (int)(sizeof(first_syllables) / sizeof(first_syllables[0]));
    int second_count = (int)(sizeof(second_syllables) / sizeof(second_syllables[0]));
    int surname_idx = (id - 1) % surname_count;
    int given_combo = ((id - 1) / surname_count) % (first_count * second_count);
    int first_idx = given_combo / second_count;
    int second_idx = given_combo % second_count;

    snprintf(buffer, buffer_size, "%s%s%s",
             surnames[surname_idx], first_syllables[first_idx], second_syllables[second_idx]);
}

static void build_jungle_row_data(int id, char *buffer, size_t buffer_size) {
    const char *track = jungle_track_for_id(id);
    const char *background = jungle_background_for_id(id);
    int pretest = jungle_pretest_for_id(id);
    const char *status = jungle_status_for_id(id, pretest);
    char email[64];
    char phone[32];
    char name[64];
    char history[64];
    char github[32];

    build_jungle_email(id, email, sizeof(email));
    build_jungle_phone(id, phone, sizeof(phone));
    build_jungle_name(id, name, sizeof(name));
    build_jungle_history(id, background, history, sizeof(history));
    build_jungle_github(id, background, github, sizeof(github));
    snprintf(buffer, buffer_size, "%s,%s,%s,%s,%s,%s,%d,%s,%s,2026_spring",
             email, phone, name, track, background, history, pretest, github, status);
}

static void build_jungle_csv_record(int id, char *buffer, size_t buffer_size) {
    const char *track = jungle_track_for_id(id);
    const char *background = jungle_background_for_id(id);
    int pretest = jungle_pretest_for_id(id);
    const char *status = jungle_status_for_id(id, pretest);
    char email[64];
    char phone[32];
    char name[64];
    char history[64];
    char github[32];

    build_jungle_email(id, email, sizeof(email));
    build_jungle_phone(id, phone, sizeof(phone));
    build_jungle_name(id, name, sizeof(name));
    build_jungle_history(id, background, history, sizeof(history));
    build_jungle_github(id, background, github, sizeof(github));
    snprintf(buffer, buffer_size, "%d,%s,%s,%s,%s,%s,%s,%d,%s,%s,2026_spring",
             id, email, phone, name, track, background, history, pretest, github, status);
}

void generate_jungle_dataset(int record_count, const char *filename) {
    FILE *f;
    int i;
    const char *output = (filename && filename[0]) ? filename : JUNGLE_BENCHMARK_CSV;

    record_count = clamp_record_count(record_count <= 0 ? 1000000 : record_count, 1);
    if (path_exists(output)) {
        printf("[safe-stop] dataset file already exists: %s\n", output);
        printf("[notice] No CSV files were overwritten. Choose a new filename or remove it manually.\n");
        return;
    }

    f = fopen(output, "wb");
    if (!f) {
        printf("[error] dataset file could not be created: %s\n", output);
        return;
    }
    if (fputs(JUNGLE_BENCHMARK_HEADER, f) == EOF) {
        fclose(f);
        printf("[error] dataset header could not be written: %s\n", output);
        return;
    }

    for (i = 1; i <= record_count; i++) {
        char row[512];
        build_jungle_csv_record(i, row, sizeof(row));
        if (fputs(row, f) == EOF || fputc('\n', f) == EOF) {
            fclose(f);
            printf("[error] dataset write failed at row %d.\n", i);
            return;
        }
    }

    if (fclose(f) != 0) {
        printf("[error] dataset file close failed: %s\n", output);
        return;
    }

    printf("[ok] jungle applicant dataset generated: %s (%d rows)\n", output, record_count);
}

static int unique_index_insert(TableCache *tc, UniqueIndex *index, const char *key, int row_index) {
    int existing_row;
    int result;

    if (!index || !key || strlen(key) == 0) return 1;
    if (bptree_string_search(index->tree, key, &existing_row)) {
        if (!tc || (existing_row != row_index && slot_is_active(tc, existing_row))) return 0;
        if (!bptree_string_delete(index->tree, key)) {
            int uk_slot;
            int col_idx = index->col_idx;
            if (!rebuild_uk_indexes(tc)) return 0;
            uk_slot = get_uk_slot(tc, col_idx);
            if (uk_slot == -1) return 0;
            index = tc->uk_indexes[uk_slot];
        }
    }
    result = bptree_string_insert(index->tree, key, row_index);
    return result == 1 ? 1 : result;
}

void trim_and_unquote(char *str) {
    char *start;
    char *end;

    if (!str) return;
    start = str;
    while (isspace((unsigned char)*start)) start++;
    end = start + strlen(start);
    while (end > start && isspace((unsigned char)*(end - 1))) end--;
    *end = '\0';

    if (start[0] == '\'' && end > start + 1 && *(end - 1) == '\'') {
        start++;
        *(end - 1) = '\0';
    }
    if (start != str) memmove(str, start, strlen(start) + 1);
}

int compare_value(const char *field, const char *search_val) {
    char f_buf[256];
    char v_buf[256];

    strncpy(f_buf, field ? field : "", sizeof(f_buf) - 1);
    f_buf[sizeof(f_buf) - 1] = '\0';
    strncpy(v_buf, search_val ? search_val : "", sizeof(v_buf) - 1);
    v_buf[sizeof(v_buf) - 1] = '\0';
    trim_and_unquote(f_buf);
    trim_and_unquote(v_buf);
    return strcmp(f_buf, v_buf) == 0;
}

static void copy_csv_field_range(const char *start, size_t len, char *dest, size_t dest_size) {
    if (!dest || dest_size == 0) return;
    if (!start) {
        dest[0] = '\0';
        return;
    }
    if (len >= dest_size) len = dest_size - 1;
    memcpy(dest, start, len);
    dest[len] = '\0';
    trim_and_unquote(dest);
}

static int row_field_equals(TableCache *tc, const char *row, int col_idx, const char *search_val) {
    const char *field_start;
    const char *p;
    int current_col = 0;
    int in_quotes = 0;
    char field_buf[RECORD_SIZE];
    char value_buf[RECORD_SIZE];

    (void)tc;
    if (!row || col_idx < 0 || !search_val) return 0;
    field_start = row;
    p = row;
    while (1) {
        if (*p == '\'') in_quotes = !in_quotes;
        if ((*p == ',' && !in_quotes) || *p == '\0' || *p == '\r' || *p == '\n') {
            if (current_col == col_idx) {
                copy_csv_field_range(field_start, (size_t)(p - field_start), field_buf, sizeof(field_buf));
                normalize_value(search_val, value_buf, sizeof(value_buf));
                return strcmp(field_buf, value_buf) == 0;
            }
            if (*p == '\0' || *p == '\r' || *p == '\n') break;
            current_col++;
            field_start = p + 1;
        }
        p++;
    }
    return 0;
}

static void normalize_value(const char *src, char *dest, size_t dest_size) {
    strncpy(dest, src ? src : "", dest_size - 1);
    dest[dest_size - 1] = '\0';
    trim_and_unquote(dest);
}

void parse_csv_row(const char *row, char *fields[MAX_COLS], char *buffer) {
    int i = 0;
    int in_quotes = 0;
    char *p;

    strncpy(buffer, row ? row : "", RECORD_SIZE - 1);
    buffer[RECORD_SIZE - 1] = '\0';
    p = buffer;
    fields[i++] = p;

    while (*p && i < MAX_COLS) {
        if (*p == '\'') in_quotes = !in_quotes;
        if (*p == ',' && !in_quotes) {
            *p = '\0';
            fields[i++] = p + 1;
        }
        p++;
    }
}

static int get_col_idx(TableCache *tc, const char *col_name) {
    int i;
    if (!col_name || strlen(col_name) == 0) return -1;
    for (i = 0; i < tc->col_count; i++) {
        if (strcmp(tc->cols[i].name, col_name) == 0) return i;
    }
    return -1;
}

static int get_uk_slot(TableCache *tc, int col_idx) {
    int i;
    for (i = 0; i < tc->uk_count; i++) {
        if (tc->uk_indices[i] == col_idx) return i;
    }
    return -1;
}

static int ensure_record_capacity(TableCache *tc, int required) {
    int new_capacity;
    char **new_records;
    int *new_active;
    long *new_row_ids;
    RowRef *new_row_refs;
    long *new_offsets;
    unsigned char *new_store;
    unsigned char *new_cached;
    unsigned long long *new_cache_seq;

    if (required <= tc->record_capacity) return 1;
    new_capacity = tc->record_capacity > 0 ? tc->record_capacity : INITIAL_RECORD_CAPACITY;
    while (new_capacity < required) {
        if (new_capacity > MAX_RECORDS / 2) {
            new_capacity = MAX_RECORDS;
            break;
        }
        new_capacity *= 2;
    }
    if (required > new_capacity) return 0;

    new_records = (char **)realloc(tc->records, (size_t)new_capacity * sizeof(char *));
    if (!new_records) return 0;
    new_active = (int *)realloc(tc->record_active, (size_t)new_capacity * sizeof(int));
    if (!new_active) {
        tc->records = new_records;
        return 0;
    }
    new_row_ids = (long *)realloc(tc->row_ids, (size_t)new_capacity * sizeof(long));
    if (!new_row_ids) {
        tc->records = new_records;
        tc->record_active = new_active;
        return 0;
    }
    new_row_refs = (RowRef *)realloc(tc->row_refs, (size_t)new_capacity * sizeof(RowRef));
    if (!new_row_refs) {
        tc->records = new_records;
        tc->record_active = new_active;
        tc->row_ids = new_row_ids;
        return 0;
    }
    new_offsets = (long *)realloc(tc->row_offsets, (size_t)new_capacity * sizeof(long));
    if (!new_offsets) {
        tc->records = new_records;
        tc->record_active = new_active;
        tc->row_ids = new_row_ids;
        tc->row_refs = new_row_refs;
        return 0;
    }
    new_store = (unsigned char *)realloc(tc->row_store, (size_t)new_capacity * sizeof(unsigned char));
    if (!new_store) {
        tc->records = new_records;
        tc->record_active = new_active;
        tc->row_ids = new_row_ids;
        tc->row_refs = new_row_refs;
        tc->row_offsets = new_offsets;
        return 0;
    }
    new_cached = (unsigned char *)realloc(tc->row_cached, (size_t)new_capacity * sizeof(unsigned char));
    if (!new_cached) {
        tc->records = new_records;
        tc->record_active = new_active;
        tc->row_ids = new_row_ids;
        tc->row_refs = new_row_refs;
        tc->row_offsets = new_offsets;
        tc->row_store = new_store;
        return 0;
    }
    new_cache_seq = (unsigned long long *)realloc(tc->row_cache_seq,
                                                  (size_t)new_capacity * sizeof(unsigned long long));
    if (!new_cache_seq) {
        tc->records = new_records;
        tc->record_active = new_active;
        tc->row_ids = new_row_ids;
        tc->row_refs = new_row_refs;
        tc->row_offsets = new_offsets;
        tc->row_store = new_store;
        tc->row_cached = new_cached;
        return 0;
    }
    if (new_capacity > tc->record_capacity) {
        memset(new_records + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(char *));
        memset(new_active + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(int));
        memset(new_row_ids + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(long));
        memset(new_row_refs + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(RowRef));
        memset(new_offsets + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(long));
        memset(new_store + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(unsigned char));
        memset(new_cached + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(unsigned char));
        memset(new_cache_seq + tc->record_capacity, 0,
               (size_t)(new_capacity - tc->record_capacity) * sizeof(unsigned long long));
    }
    tc->records = new_records;
    tc->record_active = new_active;
    tc->row_ids = new_row_ids;
    tc->row_refs = new_row_refs;
    tc->row_offsets = new_offsets;
    tc->row_store = new_store;
    tc->row_cached = new_cached;
    tc->row_cache_seq = new_cache_seq;
    tc->record_capacity = new_capacity;
    return 1;
}

static int ensure_free_slot_capacity(TableCache *tc, int required) {
    int new_capacity;
    int *new_slots;

    if (required <= tc->free_capacity) return 1;
    new_capacity = tc->free_capacity > 0 ? tc->free_capacity : INITIAL_RECORD_CAPACITY;
    while (new_capacity < required) new_capacity *= 2;
    new_slots = (int *)realloc(tc->free_slots, (size_t)new_capacity * sizeof(int));
    if (!new_slots) return 0;
    tc->free_slots = new_slots;
    tc->free_capacity = new_capacity;
    return 1;
}

static int push_free_slot(TableCache *tc, int slot_id) {
    if (!ensure_free_slot_capacity(tc, tc->free_count + 1)) return 0;
    tc->free_slots[tc->free_count++] = slot_id;
    return 1;
}

static int take_record_slot(TableCache *tc, int allow_reuse, int *slot_id) {
    if (!tc || !slot_id) return 0;
    if (allow_reuse && tc->free_count > 0) {
        *slot_id = tc->free_slots[--tc->free_count];
        return 1;
    }
    if (tc->record_count >= MAX_RECORDS) return 0;
    if (!ensure_record_capacity(tc, tc->record_count + 1)) return 0;
    *slot_id = tc->record_count++;
    return 1;
}

static int ensure_tail_index_capacity(TableCache *tc, int required) {
    int new_capacity;
    long *new_ids;
    long *new_offsets;

    if (required <= tc->tail_capacity) return 1;
    new_capacity = tc->tail_capacity > 0 ? tc->tail_capacity : INITIAL_RECORD_CAPACITY;
    while (new_capacity < required) new_capacity *= 2;
    new_ids = (long *)realloc(tc->tail_pk_ids, (size_t)new_capacity * sizeof(long));
    if (!new_ids) return 0;
    new_offsets = (long *)realloc(tc->tail_offsets, (size_t)new_capacity * sizeof(long));
    if (!new_offsets) {
        tc->tail_pk_ids = new_ids;
        return 0;
    }
    tc->tail_pk_ids = new_ids;
    tc->tail_offsets = new_offsets;
    tc->tail_capacity = new_capacity;
    return 1;
}

static int append_tail_pk_offset(TableCache *tc, long id_value, long offset) {
    int pos;

    if (!tc) return 1;
    if (!ensure_tail_index_capacity(tc, tc->tail_count + 1)) return 0;
    pos = tc->tail_count;
    while (pos > 0 && tc->tail_pk_ids[pos - 1] > id_value) {
        tc->tail_pk_ids[pos] = tc->tail_pk_ids[pos - 1];
        tc->tail_offsets[pos] = tc->tail_offsets[pos - 1];
        pos--;
    }
    tc->tail_pk_ids[pos] = id_value;
    tc->tail_offsets[pos] = offset;
    tc->tail_count++;
    return 1;
}

static int find_tail_pk_offset(TableCache *tc, long id_value, long *offset) {
    int lo = 0;
    int hi;

    if (!tc || !offset || tc->tail_count <= 0) return 0;
    hi = tc->tail_count - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (tc->tail_pk_ids[mid] == id_value) {
            *offset = tc->tail_offsets[mid];
            return 1;
        }
        if (tc->tail_pk_ids[mid] < id_value) lo = mid + 1;
        else hi = mid - 1;
    }
    return 0;
}

static int slot_is_active(TableCache *tc, int slot_id) {
    return tc && slot_id >= 0 && slot_id < tc->record_count &&
           tc->record_active && tc->record_active[slot_id];
}

static void clear_page_cache(TableCache *tc) {
    int i;

    if (!tc) return;
    for (i = 0; i < PAGE_CACHE_LIMIT; i++) {
        free(tc->page_cache[i].data);
        tc->page_cache[i].data = NULL;
        tc->page_cache[i].page_start = 0;
        tc->page_cache[i].bytes = 0;
        tc->page_cache[i].valid = 0;
        tc->page_cache[i].last_used = 0;
    }
    tc->page_cache_count = 0;
    tc->page_cache_clock = 0;
}

static PageCacheEntry *get_page_cache_entry(TableCache *tc, long offset) {
    long page_start;
    int i;
    int target = -1;
    unsigned long long oldest = 0;
    PageCacheEntry *entry;

    if (!tc || !tc->file || offset < 0) return NULL;
    page_start = (offset / PAGE_CACHE_PAGE_SIZE) * PAGE_CACHE_PAGE_SIZE;

    for (i = 0; i < PAGE_CACHE_LIMIT; i++) {
        if (tc->page_cache[i].valid && tc->page_cache[i].page_start == page_start) {
            tc->page_cache[i].last_used = ++tc->page_cache_clock;
            return &tc->page_cache[i];
        }
    }

    for (i = 0; i < PAGE_CACHE_LIMIT; i++) {
        if (!tc->page_cache[i].valid) {
            target = i;
            break;
        }
        if (target == -1 || tc->page_cache[i].last_used < oldest) {
            target = i;
            oldest = tc->page_cache[i].last_used;
        }
    }
    if (target < 0) return NULL;

    entry = &tc->page_cache[target];
    if (!entry->data) {
        entry->data = (char *)malloc(PAGE_CACHE_PAGE_SIZE + 1);
        if (!entry->data) return NULL;
    }
    if (fflush(tc->file) != 0) return NULL;
    if (fseek(tc->file, page_start, SEEK_SET) != 0) return NULL;
    entry->bytes = fread(entry->data, 1, PAGE_CACHE_PAGE_SIZE, tc->file);
    if (ferror(tc->file)) {
        clearerr(tc->file);
        return NULL;
    }
    entry->data[entry->bytes] = '\0';
    entry->page_start = page_start;
    entry->valid = 1;
    entry->last_used = ++tc->page_cache_clock;
    if (tc->page_cache_count < PAGE_CACHE_LIMIT) tc->page_cache_count++;
    fseek(tc->file, 0, SEEK_END);
    return entry;
}

static char *read_row_from_page_cache(TableCache *tc, long offset) {
    PageCacheEntry *entry;
    size_t in_page;
    size_t len = 0;
    char *row;

    entry = get_page_cache_entry(tc, offset);
    if (!entry || offset < entry->page_start) return NULL;
    in_page = (size_t)(offset - entry->page_start);
    if (in_page >= entry->bytes) return NULL;

    while (in_page + len < entry->bytes &&
           entry->data[in_page + len] != '\n' &&
           entry->data[in_page + len] != '\r') {
        len++;
    }
    if (in_page + len >= entry->bytes && len == PAGE_CACHE_PAGE_SIZE - in_page) {
        return NULL;
    }

    row = (char *)malloc(len + 1);
    if (!row) return NULL;
    memcpy(row, entry->data + in_page, len);
    row[len] = '\0';
    return row;
}

static char *slot_row(TableCache *tc, int slot_id) {
    char line[RECORD_SIZE];
    char *nl;
    long offset;

    if (!slot_is_active(tc, slot_id)) return NULL;
    if (tc->records[slot_id]) {
        if (tc->row_store && tc->row_store[slot_id] != ROW_STORE_MEMORY) {
            if (tc->row_cached) tc->row_cached[slot_id] = 1;
            if (tc->row_cache_seq) tc->row_cache_seq[slot_id] = ++tc->row_cache_clock;
        }
        return tc->records[slot_id];
    }
    if (!tc->row_refs || tc->row_refs[slot_id].store != ROW_STORE_CSV ||
        tc->row_refs[slot_id].offset < 0 || !tc->file) {
        return NULL;
    }
    if (!evict_row_cache_if_needed(tc)) return NULL;
    offset = tc->row_refs[slot_id].offset;
    tc->records[slot_id] = read_row_from_page_cache(tc, offset);
    if (!tc->records[slot_id]) {
        if (fflush(tc->file) != 0) return NULL;
        if (fseek(tc->file, offset, SEEK_SET) != 0) return NULL;
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return NULL;
        }
        fseek(tc->file, 0, SEEK_END);
        nl = strpbrk(line, "\r\n");
        if (nl) *nl = '\0';
        tc->records[slot_id] = dup_string(line);
    }
    if (!tc->records[slot_id]) return NULL;
    if (tc->row_cached) tc->row_cached[slot_id] = 1;
    if (tc->row_cache_seq) tc->row_cache_seq[slot_id] = ++tc->row_cache_clock;
    tc->cached_record_count++;
    return tc->records[slot_id];
}

static char *slot_row_scan(TableCache *tc, int slot_id, int *owned) {
    char line[RECORD_SIZE];
    char *nl;
    long offset;
    char *row;

    if (owned) *owned = 0;
    if (!slot_is_active(tc, slot_id)) return NULL;
    if (tc->records && tc->records[slot_id]) return tc->records[slot_id];
    if (!tc->row_refs || tc->row_refs[slot_id].store != ROW_STORE_CSV ||
        tc->row_refs[slot_id].offset < 0 || !tc->file) {
        return NULL;
    }

    offset = tc->row_refs[slot_id].offset;
    row = read_row_from_page_cache(tc, offset);
    if (!row) {
        if (fflush(tc->file) != 0) return NULL;
        if (fseek(tc->file, offset, SEEK_SET) != 0) return NULL;
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return NULL;
        }
        fseek(tc->file, 0, SEEK_END);
        nl = strpbrk(line, "\r\n");
        if (nl) *nl = '\0';
        row = dup_string(line);
    }
    if (owned && row) *owned = 1;
    return row;
}

static int evict_row_cache_if_needed(TableCache *tc) {
    int i;
    int scanned;

    if (!tc) return 0;
    while (tc->cached_record_count >= ROW_CACHE_LIMIT) {
        if (tc->record_count <= 0) return 1;
        scanned = 0;
        while (scanned < tc->record_count) {
            i = tc->row_cache_evict_cursor;
            tc->row_cache_evict_cursor++;
            if (tc->row_cache_evict_cursor >= tc->record_count) tc->row_cache_evict_cursor = 0;
            scanned++;
            if (!tc->row_cached || !tc->row_cached[i] || !tc->records[i]) continue;
            if (tc->row_store && tc->row_store[i] == ROW_STORE_MEMORY) continue;
            free(tc->records[i]);
            tc->records[i] = NULL;
            tc->row_cached[i] = 0;
            tc->row_cache_seq[i] = 0;
            tc->cached_record_count--;
            break;
        }
        if (scanned >= tc->record_count) return 1;
    }
    return 1;
}

static int assign_slot_row(TableCache *tc, int slot_id, const char *row,
                           RowStoreType store_type, long offset, int cache_row) {
    if (!tc || slot_id < 0 || slot_id >= tc->record_capacity) return 0;
    free(tc->records[slot_id]);
    if (tc->row_cached && tc->row_cached[slot_id] && tc->cached_record_count > 0) {
        tc->cached_record_count--;
    }
    tc->records[slot_id] = NULL;
    tc->row_cached[slot_id] = 0;
    tc->row_cache_seq[slot_id] = 0;
    tc->row_store[slot_id] = (unsigned char)store_type;
    tc->row_offsets[slot_id] = offset;
    if (tc->row_refs) {
        tc->row_refs[slot_id].store = store_type;
        tc->row_refs[slot_id].offset = offset;
    }
    if (!cache_row) return 1;
    if (store_type != ROW_STORE_MEMORY && !evict_row_cache_if_needed(tc)) return 0;
    tc->records[slot_id] = dup_string(row);
    if (!tc->records[slot_id]) return 0;
    if (store_type != ROW_STORE_MEMORY) {
        tc->row_cached[slot_id] = 1;
        tc->row_cache_seq[slot_id] = ++tc->row_cache_clock;
        tc->cached_record_count++;
    }
    return 1;
}

static int deactivate_slot(TableCache *tc, int slot_id, int add_to_free_list) {
    if (!slot_is_active(tc, slot_id)) return 0;
    if (add_to_free_list && !ensure_free_slot_capacity(tc, tc->free_count + 1)) return 0;
    free(tc->records[slot_id]);
    tc->records[slot_id] = NULL;
    if (tc->row_ids) tc->row_ids[slot_id] = 0;
    if (tc->row_offsets) tc->row_offsets[slot_id] = 0;
    if (tc->row_store) tc->row_store[slot_id] = ROW_STORE_NONE;
    if (tc->row_refs) {
        tc->row_refs[slot_id].store = ROW_STORE_NONE;
        tc->row_refs[slot_id].offset = 0;
    }
    if (tc->row_cached && tc->row_cached[slot_id] && tc->cached_record_count > 0) tc->cached_record_count--;
    if (tc->row_cached) tc->row_cached[slot_id] = 0;
    if (tc->row_cache_seq) tc->row_cache_seq[slot_id] = 0;
    tc->record_active[slot_id] = 0;
    tc->active_count--;
    if (add_to_free_list) tc->free_slots[tc->free_count++] = slot_id;
    return 1;
}

static int append_record_raw_memory(TableCache *tc, const char *row, long row_id,
                                    long row_offset, int *inserted_slot) {
    int slot_id;
    int cache_row;

    if (!take_record_slot(tc, 0, &slot_id)) return 0;
    cache_row = 0;
    if (!assign_slot_row(tc, slot_id, row, ROW_STORE_CSV, row_offset, cache_row)) {
        push_free_slot(tc, slot_id);
        return 0;
    }
    tc->row_ids[slot_id] = row_id;
    tc->record_active[slot_id] = 1;
    tc->active_count++;
    if (inserted_slot) *inserted_slot = slot_id;
    return 1;
}

static int append_record_csv_indexed(TableCache *tc, const char *row, long id_value,
                                     long row_offset, int *inserted_slot) {
    int slot_id;
    long row_id;
    int cache_row;

    if (!take_record_slot(tc, 1, &slot_id)) return 0;
    row_id = tc->next_row_id++;
    cache_row = 0;
    if (!assign_slot_row(tc, slot_id, row, ROW_STORE_CSV, row_offset, cache_row)) {
        push_free_slot(tc, slot_id);
        return 0;
    }
    tc->row_ids[slot_id] = row_id;
    tc->record_active[slot_id] = 1;
    tc->active_count++;

    {
        long index_key = tc->pk_idx != -1 ? id_value : row_id;
        int existing_row;
        if (bptree_search(tc->id_index, index_key, &existing_row)) {
            if ((existing_row != slot_id && slot_is_active(tc, existing_row)) ||
                (!bptree_delete(tc->id_index, index_key) && !rebuild_id_index(tc))) {
                deactivate_slot(tc, slot_id, 1);
                return 0;
            }
        }
    }
    if (bptree_insert(tc->id_index, tc->pk_idx != -1 ? id_value : row_id, slot_id) != 1 ||
        !index_record_uks_from_row(tc, row, slot_id)) {
        deactivate_slot(tc, slot_id, 1);
        return 0;
    }
    if (tc->pk_idx != -1 && id_value >= tc->next_auto_id) tc->next_auto_id = id_value + 1;
    if (inserted_slot) *inserted_slot = slot_id;
    return 1;
}

static int append_record_memory(TableCache *tc, const char *row, long id_value, int *inserted_slot) {
    int slot_id;
    long row_id;

    if (!take_record_slot(tc, 1, &slot_id)) return 0;
    if (!assign_slot_row(tc, slot_id, row, ROW_STORE_MEMORY, -1, 1)) {
        push_free_slot(tc, slot_id);
        return 0;
    }
    row_id = tc->next_row_id++;
    tc->row_ids[slot_id] = row_id;
    tc->record_active[slot_id] = 1;
    tc->active_count++;

    {
        long index_key = tc->pk_idx != -1 ? id_value : row_id;
        int existing_row;
        if (bptree_search(tc->id_index, index_key, &existing_row)) {
            if ((existing_row != slot_id && slot_is_active(tc, existing_row)) ||
                (!bptree_delete(tc->id_index, index_key) && !rebuild_id_index(tc))) {
                deactivate_slot(tc, slot_id, 1);
                return 0;
            }
        }
    }
    if (bptree_insert(tc->id_index, tc->pk_idx != -1 ? id_value : row_id, slot_id) != 1) {
        deactivate_slot(tc, slot_id, 1);
        return 0;
    }
    if (tc->pk_idx != -1) {
        if (id_value >= tc->next_auto_id) tc->next_auto_id = id_value + 1;
    }
    if (!index_record_uks(tc, slot_id)) {
        deactivate_slot(tc, slot_id, 1);
        if (!rebuild_id_index(tc) || !rebuild_uk_indexes(tc)) {
            printf("[error] INSERT rollback failed: indexes may be stale.\n");
        }
        return 0;
    }
    if (inserted_slot) *inserted_slot = slot_id;
    return 1;
}

static void rollback_updated_records(TableCache *tc, char **old_records) {
    int i;

    if (!tc || !old_records) return;
    for (i = 0; i < tc->record_count; i++) {
        if (old_records[i]) {
            free(tc->records[i]);
            tc->records[i] = old_records[i];
            tc->row_store[i] = ROW_STORE_MEMORY;
            tc->row_offsets[i] = -1;
            if (tc->row_refs) {
                tc->row_refs[i].store = ROW_STORE_MEMORY;
                tc->row_refs[i].offset = -1;
            }
            tc->row_cached[i] = 0;
            tc->row_cache_seq[i] = 0;
            tc->record_active[i] = 1;
            old_records[i] = NULL;
        }
    }
}

static void free_table_storage(TableCache *tc) {
    int i;

    if (!tc) return;
    if (tc->col_count > 0 && tc->records) {
        if (!save_index_snapshot(tc)) {
            INFO_PRINTF("[warning] failed to save index snapshot for table '%s'.\n", tc->table_name);
        }
    }
    if (tc->file) {
        fclose(tc->file);
        tc->file = NULL;
    }
    if (tc->delta_file) {
        if (!close_delta_batch(tc)) {
            printf("[warning] delta log batch close failed for table '%s'.\n", tc->table_name);
        }
        fclose(tc->delta_file);
        tc->delta_file = NULL;
    }
    tc->delta_batch_open = 0;
    tc->delta_ops_since_compact_check = 0;
    tc->delta_bytes_since_compact = 0;
    for (i = 0; i < tc->record_count; i++) free(tc->records[i]);
    clear_page_cache(tc);
    free(tc->records);
    free(tc->row_ids);
    free(tc->row_refs);
    free(tc->row_offsets);
    free(tc->row_store);
    free(tc->row_cached);
    free(tc->row_cache_seq);
    free(tc->record_active);
    free(tc->free_slots);
    free(tc->tail_pk_ids);
    free(tc->tail_offsets);
    tc->records = NULL;
    tc->row_ids = NULL;
    tc->row_refs = NULL;
    tc->row_offsets = NULL;
    tc->row_store = NULL;
    tc->row_cached = NULL;
    tc->row_cache_seq = NULL;
    tc->record_active = NULL;
    tc->free_slots = NULL;
    tc->tail_pk_ids = NULL;
    tc->tail_offsets = NULL;
    tc->record_capacity = 0;
    tc->record_count = 0;
    tc->active_count = 0;
    tc->cached_record_count = 0;
    tc->row_cache_clock = 0;
    tc->row_cache_evict_cursor = 0;
    tc->free_count = 0;
    tc->free_capacity = 0;
    tc->tail_count = 0;
    tc->tail_capacity = 0;
    bptree_destroy(tc->id_index);
    tc->id_index = NULL;
    for (i = 0; i < tc->uk_count; i++) {
        unique_index_destroy(tc->uk_indexes[i]);
        tc->uk_indexes[i] = NULL;
    }
    tc->uk_count = 0;
}

static void reset_table_cache(TableCache *tc) {
    memset(tc, 0, sizeof(TableCache));
    tc->file = NULL;
    tc->delta_file = NULL;
    tc->pk_idx = -1;
    tc->next_auto_id = 1;
    tc->next_row_id = 1;
    tc->append_offset = -1;
    tc->id_index = bptree_create();
}

static void touch_table(TableCache *tc) {
    tc->last_used_seq = ++g_table_access_seq;
}

static int find_lru_table_index(void) {
    int i;
    int lru_index = 0;
    unsigned long long oldest_seq = open_tables[0].last_used_seq;

    for (i = 1; i < open_table_count; i++) {
        if (open_tables[i].last_used_seq < oldest_seq) {
            oldest_seq = open_tables[i].last_used_seq;
            lru_index = i;
        }
    }
    return lru_index;
}

static int parse_long_value(const char *value, long *out) {
    char buf[256];
    char *endptr;

    strncpy(buf, value ? value : "", sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    trim_and_unquote(buf);
    if (strlen(buf) == 0 || strcmp(buf, "NULL") == 0) return 0;

    *out = strtol(buf, &endptr, 10);
    return endptr != buf && *endptr == '\0';
}

static int append_record_file(TableCache *tc, const char *row, int flush_now) {
    size_t row_len;

    if (!tc->file) return 0;
    row_len = strlen(row);
    if (fprintf(tc->file, "%s\n", row) < 0) return 0;
    if (flush_now && fflush(tc->file) != 0) return 0;
    if (ferror(tc->file)) return 0;
    if (tc->append_offset >= 0) tc->append_offset += (long)row_len + 1;
    return 1;
}

static int for_each_file_row_from(TableCache *tc, long start_offset,
                                  int (*visitor)(TableCache *, const char *, void *),
                                  void *ctx) {
    char line[RECORD_SIZE];

    if (!tc || !tc->file || !visitor) return 0;
    if (fflush(tc->file) != 0) return 0;
    if (start_offset > 0) {
        if (fseek(tc->file, start_offset, SEEK_SET) != 0) return 0;
    } else {
        if (fseek(tc->file, 0, SEEK_SET) != 0) return 0;
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return 1;
        }
    }

    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        size_t line_len = strlen(line);

        if (!nl && line_len == sizeof(line) - 1 && !feof(tc->file)) {
            printf("[error] row too long while scanning '%s' (max %d bytes).\n",
                   tc->table_name, RECORD_SIZE - 1);
            fseek(tc->file, 0, SEEK_END);
            return 0;
        }
        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        if (!visitor(tc, line, ctx)) {
            fseek(tc->file, 0, SEEK_END);
            return 0;
        }
    }
    fseek(tc->file, 0, SEEK_END);
    return 1;
}

typedef struct {
    int col_idx;
    const char *value;
    int found;
} FileValueSearch;

static int find_value_visitor(TableCache *tc, const char *row, void *ctx) {
    FileValueSearch *search = (FileValueSearch *)ctx;
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};

    parse_csv_row(row, fields, row_buf);
    if (search->col_idx >= 0 && search->col_idx < tc->col_count &&
        compare_value(fields[search->col_idx], search->value)) {
        search->found = 1;
        return 0;
    }
    return 1;
}

static int table_file_has_value(TableCache *tc, int col_idx, const char *value) {
    FileValueSearch search;
    long start_offset = (tc && tc->cache_truncated) ? tc->uncached_start_offset : 0;

    if (!tc || col_idx < 0 || !value || strlen(value) == 0) return 0;
    search.col_idx = col_idx;
    search.value = value;
    search.found = 0;
    if (!for_each_file_row_from(tc, start_offset, find_value_visitor, &search) && !search.found) return 0;
    return search.found;
}

static int replace_table_file(const char *temp_filename, const char *filename) {
#if defined(_WIN32)
    return MoveFileExA(temp_filename, filename, MOVEFILE_REPLACE_EXISTING) != 0;
#else
    return rename(temp_filename, filename) == 0;
#endif
}

static int write_table_header(FILE *out, TableCache *tc) {
    int i;

    for (i = 0; i < tc->col_count; i++) {
        if (fprintf(out, "%s", tc->cols[i].name) < 0) return 0;
        if (tc->cols[i].type == COL_PK && fprintf(out, "(PK)") < 0) return 0;
        else if (tc->cols[i].type == COL_UK && fprintf(out, "(UK)") < 0) return 0;
        else if (tc->cols[i].type == COL_NN && fprintf(out, "(NN)") < 0) return 0;
        if (fprintf(out, "%s", (i == tc->col_count - 1 ? "\n" : ",")) < 0) return 0;
    }
    return 1;
}

typedef struct {
    char type;
    long id;
    char *row;
} DeltaOp;

static void get_delta_filename(TableCache *tc, char *filename, size_t filename_size) {
    snprintf(filename, filename_size, "%s.delta", tc->table_name);
}

static int delta_log_exists(TableCache *tc) {
    char filename[300];
    FILE *f;

    if (!tc) return 0;
    get_delta_filename(tc, filename, sizeof(filename));
    f = fopen(filename, "r");
    if (!f) return 0;
    fclose(f);
    return 1;
}

static int clear_delta_log(TableCache *tc) {
    char filename[300];

    if (!tc) return 0;
    if (tc->delta_file) {
        close_delta_batch(tc);
        fclose(tc->delta_file);
        tc->delta_file = NULL;
    }
    tc->delta_batch_open = 0;
    get_delta_filename(tc, filename, sizeof(filename));
    remove(filename);
    tc->delta_ops_since_compact_check = 0;
    tc->delta_bytes_since_compact = 0;
    return 1;
}

static FILE *get_delta_writer(TableCache *tc) {
    char filename[300];

    if (!tc) return NULL;
    if (tc->delta_file) return tc->delta_file;
    get_delta_filename(tc, filename, sizeof(filename));
    tc->delta_file = fopen(filename, "a");
    if (tc->delta_file) setvbuf(tc->delta_file, NULL, _IOFBF, TABLE_FILE_BUFFER_SIZE);
    return tc->delta_file;
}

static int can_use_delta_log(TableCache *tc) {
    return tc && tc->id_index != NULL;
}

static int begin_delta_batch(TableCache *tc) {
    FILE *f;
    int written;

    if (!tc) return 0;
    if (tc->delta_batch_open) return 1;
    f = get_delta_writer(tc);
    if (!f) return 0;
    written = fprintf(f, "B\n");
    if (written < 0) return 0;
    tc->delta_bytes_since_compact += written;
    tc->delta_batch_open = 1;
    return 1;
}

static int close_delta_batch(TableCache *tc) {
    int written;

    if (!tc || !tc->delta_file || !tc->delta_batch_open) return 1;
    written = fprintf(tc->delta_file, "E\n");
    if (written < 0) return 0;
    tc->delta_bytes_since_compact += written;
    tc->delta_batch_open = 0;
    return fflush(tc->delta_file) == 0 && !ferror(tc->delta_file);
}

static int track_delta_write(TableCache *tc, int written) {
    if (!tc || written < 0) return 0;
    tc->delta_bytes_since_compact += written;
    return 1;
}

static long delta_log_size(TableCache *tc) {
    char filename[300];
    FILE *f;
    long size;

    if (!tc) return 0;
    if (!close_delta_batch(tc)) return 0;
    get_delta_filename(tc, filename, sizeof(filename));
    f = fopen(filename, "rb");
    if (!f) return 0;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return 0;
    }
    size = ftell(f);
    fclose(f);
    return size > 0 ? size : 0;
}

static int maybe_compact_delta_log(TableCache *tc) {
    long size;

    if (!tc || tc->cache_truncated) return 1;
    tc->delta_ops_since_compact_check++;
    if (tc->delta_ops_since_compact_check < DELTA_COMPACT_CHECK_INTERVAL) return 1;
    tc->delta_ops_since_compact_check = 0;
    if (tc->delta_bytes_since_compact < DELTA_COMPACT_BYTES) return 1;
    size = delta_log_size(tc);
    if (size < DELTA_COMPACT_BYTES) return 1;
    INFO_PRINTF("[delta] compacting %ld-byte delta log into CSV.\n", size);
    return rewrite_file(tc);
}

static int get_row_pk_value(TableCache *tc, const char *row, long *id_value) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};

    if (!tc || tc->pk_idx == -1 || !row || !id_value) return 0;
    parse_csv_row(row, fields, row_buf);
    return parse_long_value(fields[tc->pk_idx], id_value);
}

static int get_row_index_key(TableCache *tc, const char *row, int slot_id, long *id_value) {
    if (!tc || !row || !id_value) return 0;
    if (tc->pk_idx != -1) return get_row_pk_value(tc, row, id_value);
    if (!tc->row_ids || slot_id < 0 || slot_id >= tc->record_count ||
        tc->row_ids[slot_id] <= 0) {
        return 0;
    }
    *id_value = tc->row_ids[slot_id];
    return 1;
}

static int find_record_index_by_key(TableCache *tc, long id_value) {
    int row_index;
    int i;

    if (!tc) return -1;
    if (tc->id_index && bptree_search(tc->id_index, id_value, &row_index) &&
        slot_is_active(tc, row_index)) {
        return row_index;
    }
    for (i = 0; i < tc->record_count; i++) {
        long row_id;
        char *row = slot_row(tc, i);
        if (row && get_row_index_key(tc, row, i, &row_id) && row_id == id_value) return i;
    }
    return -1;
}

static int delete_record_at(TableCache *tc, int row_index) {
    return deactivate_slot(tc, row_index, 1);
}

static int replace_record_at(TableCache *tc, int row_index, const char *row) {
    char *copy;

    if (!slot_is_active(tc, row_index) || !row) return 0;
    copy = dup_string(row);
    if (!copy) return 0;
    if (tc->row_cached && tc->row_cached[row_index] && tc->cached_record_count > 0) {
        tc->cached_record_count--;
    }
    free(tc->records[row_index]);
    tc->records[row_index] = copy;
    tc->row_store[row_index] = ROW_STORE_MEMORY;
    tc->row_offsets[row_index] = -1;
    if (tc->row_refs) {
        tc->row_refs[row_index].store = ROW_STORE_MEMORY;
        tc->row_refs[row_index].offset = -1;
    }
    tc->row_cached[row_index] = 0;
    tc->row_cache_seq[row_index] = 0;
    return 1;
}

static void free_delta_ops(DeltaOp *ops, int count) {
    int i;

    if (!ops) return;
    for (i = 0; i < count; i++) free(ops[i].row);
    free(ops);
}

static int append_delta_op(DeltaOp **ops, int *count, int *capacity, char type, long id, const char *row) {
    DeltaOp *new_ops;

    if (*count >= *capacity) {
        int new_capacity = (*capacity == 0) ? 16 : (*capacity * 2);
        new_ops = (DeltaOp *)realloc(*ops, (size_t)new_capacity * sizeof(DeltaOp));
        if (!new_ops) return 0;
        *ops = new_ops;
        *capacity = new_capacity;
    }
    (*ops)[*count].type = type;
    (*ops)[*count].id = id;
    (*ops)[*count].row = row ? dup_string(row) : NULL;
    if (row && !(*ops)[*count].row) return 0;
    (*count)++;
    return 1;
}

static int apply_delta_ops(TableCache *tc, DeltaOp *ops, int count) {
    int i;

    for (i = 0; i < count; i++) {
        int row_index = find_record_index_by_key(tc, ops[i].id);
        if (ops[i].type == 'U') {
            if (row_index >= 0) {
                char *old_row = slot_row(tc, row_index);
                if (old_row && !remove_record_indexes(tc, old_row)) return 0;
                if (!replace_record_at(tc, row_index, ops[i].row)) return 0;
                if (!restore_record_indexes(tc, row_index)) return 0;
            }
        } else if (ops[i].type == 'D') {
            if (row_index >= 0) {
                char *old_row = slot_row(tc, row_index);
                if (old_row && !remove_record_indexes(tc, old_row)) return 0;
                if (!delete_record_at(tc, row_index)) return 0;
            }
        }
    }
    return 1;
}

static int replay_delta_log(TableCache *tc) {
    char filename[300];
    char line[DELTA_LINE_SIZE];
    FILE *f;
    DeltaOp *ops = NULL;
    int count = 0;
    int capacity = 0;
    int in_batch = 0;
    int applied_batches = 0;

    if (!tc) return 1;
    get_delta_filename(tc, filename, sizeof(filename));
    f = fopen(filename, "r");
    if (!f) return 1;

    while (fgets(line, sizeof(line), f)) {
        char *nl = strpbrk(line, "\r\n");
        size_t line_len = strlen(line);

        if (!nl && line_len == sizeof(line) - 1 && !feof(f)) {
            printf("[error] delta row too long while loading '%s'.\n", filename);
            fclose(f);
            free_delta_ops(ops, count);
            return 0;
        }
        if (nl) *nl = '\0';
        if (strcmp(line, "B") == 0) {
            free_delta_ops(ops, count);
            ops = NULL;
            count = 0;
            capacity = 0;
            in_batch = 1;
            continue;
        }
        if (strcmp(line, "E") == 0) {
            if (in_batch) {
                if (!apply_delta_ops(tc, ops, count)) {
                    fclose(f);
                    free_delta_ops(ops, count);
                    return 0;
                }
                applied_batches++;
            }
            free_delta_ops(ops, count);
            ops = NULL;
            count = 0;
            capacity = 0;
            in_batch = 0;
            continue;
        }
        if (in_batch && line[0] == 'U' && line[1] == '\t') {
            char *id_text = line + 2;
            char *row = strchr(id_text, '\t');
            long id_value;

            if (!row) continue;
            *row++ = '\0';
            if (!parse_long_value(id_text, &id_value)) continue;
            if (!append_delta_op(&ops, &count, &capacity, 'U', id_value, row)) {
                fclose(f);
                free_delta_ops(ops, count);
                return 0;
            }
        } else if (in_batch && line[0] == 'D' && line[1] == '\t') {
            long id_value;
            if (!parse_long_value(line + 2, &id_value)) continue;
            if (!append_delta_op(&ops, &count, &capacity, 'D', id_value, NULL)) {
                fclose(f);
                free_delta_ops(ops, count);
                return 0;
            }
        }
    }
    free_delta_ops(ops, count);
    fclose(f);
    if (applied_batches > 0) {
        INFO_PRINTF("[notice] replayed %d committed delta batch(es) for table '%s'.\n",
               applied_batches, tc->table_name);
    }
    return 1;
}

static int append_delta_updates(TableCache *tc, char **old_records) {
    FILE *f;
    int i;

    if (!tc || !old_records) return 0;
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) goto fail;
    for (i = 0; i < tc->record_count; i++) {
        long id_value;
        if (!old_records[i]) continue;
        if (!slot_is_active(tc, i)) continue;
        if (!get_row_index_key(tc, slot_row(tc, i), i, &id_value)) goto fail;
        if (!track_delta_write(tc, fprintf(f, "U\t%ld\t%s\n", id_value, slot_row(tc, i)))) goto fail;
    }
    if (ferror(f)) goto fail;
    return 1;

fail:
    return 0;
}

static int append_delta_update_slot(TableCache *tc, int slot_id, const char *new_record) {
    FILE *f;
    long id_value;

    if (!tc || !new_record || !slot_is_active(tc, slot_id)) return 0;
    if (!get_row_index_key(tc, new_record, slot_id, &id_value)) return 0;
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) return 0;
    if (!track_delta_write(tc, fprintf(f, "U\t%ld\t%s\n", id_value, new_record))) return 0;
    return ferror(f) ? 0 : 1;
}

static int append_delta_update_key(TableCache *tc, long id_value, const char *new_record) {
    FILE *f;

    if (!tc || !new_record) return 0;
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) return 0;
    if (!track_delta_write(tc, fprintf(f, "U\t%ld\t%s\n", id_value, new_record))) return 0;
    return ferror(f) ? 0 : 1;
}

static int append_delta_update_one(TableCache *tc, const char *new_record) {
    FILE *f;
    long id_value;

    if (!tc || !new_record) return 0;
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) return 0;
    if (tc->pk_idx != -1) {
        if (!get_row_pk_value(tc, new_record, &id_value)) return 0;
    } else {
        int i;
        for (i = 0; i < tc->record_count; i++) {
            if (slot_row(tc, i) == new_record && get_row_index_key(tc, new_record, i, &id_value)) break;
        }
        if (i == tc->record_count) return 0;
    }
    if (!track_delta_write(tc, fprintf(f, "U\t%ld\t%s\n", id_value, new_record))) return 0;
    return ferror(f) ? 0 : 1;
}

static int append_delta_deletes(TableCache *tc, char **old_records, int *delete_flags, int old_count) {
    FILE *f;
    int i;

    if (!tc || !old_records || !delete_flags) return 0;
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) goto fail;
    for (i = 0; i < old_count; i++) {
        long id_value;
        if (!delete_flags[i]) continue;
        if (!get_row_index_key(tc, old_records[i], i, &id_value)) goto fail;
        if (!track_delta_write(tc, fprintf(f, "D\t%ld\n", id_value))) goto fail;
    }
    if (ferror(f)) goto fail;
    return 1;

fail:
    return 0;
}

static int append_delta_delete_slot(TableCache *tc, int slot_id, const char *old_record) {
    FILE *f;
    long id_value;

    if (!tc || !old_record || slot_id < 0 || slot_id >= tc->record_count) return 0;
    if (!get_row_index_key(tc, old_record, slot_id, &id_value)) return 0;
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) return 0;
    if (!track_delta_write(tc, fprintf(f, "D\t%ld\n", id_value))) return 0;
    return ferror(f) ? 0 : 1;
}

static int append_delta_delete_key(TableCache *tc, long id_value) {
    FILE *f;

    if (!tc) return 0;
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) return 0;
    if (!track_delta_write(tc, fprintf(f, "D\t%ld\n", id_value))) return 0;
    return ferror(f) ? 0 : 1;
}

static int append_delta_delete_one(TableCache *tc, const char *old_record) {
    FILE *f;
    long id_value;

    int i;

    if (!tc || !old_record) return 0;
    if (tc->pk_idx != -1) {
        if (!get_row_pk_value(tc, old_record, &id_value)) return 0;
    } else {
        for (i = 0; i < tc->record_count; i++) {
            if (tc->records[i] == old_record && get_row_index_key(tc, old_record, i, &id_value)) break;
        }
        if (i == tc->record_count) return 0;
    }
    f = get_delta_writer(tc);
    if (!f) return 0;
    if (!begin_delta_batch(tc)) return 0;
    if (!track_delta_write(tc, fprintf(f, "D\t%ld\n", id_value))) return 0;
    return ferror(f) ? 0 : 1;
}

static void discard_table_cache_for_reload(TableCache *tc) {
    int i;

    if (!tc) return;
    for (i = 0; i < tc->record_count; i++) free(tc->records[i]);
    clear_page_cache(tc);
    free(tc->records);
    free(tc->row_ids);
    free(tc->row_refs);
    free(tc->row_offsets);
    free(tc->row_store);
    free(tc->row_cached);
    free(tc->row_cache_seq);
    free(tc->record_active);
    free(tc->free_slots);
    free(tc->tail_pk_ids);
    free(tc->tail_offsets);
    bptree_destroy(tc->id_index);
    for (i = 0; i < tc->uk_count; i++) {
        unique_index_destroy(tc->uk_indexes[i]);
        tc->uk_indexes[i] = NULL;
    }
}

int rewrite_file(TableCache *tc) {
    char filename[300];
    char temp_filename[320];
    char table_name[256];
    FILE *out;
    int i;

    if (tc->file) fclose(tc->file);
    tc->file = NULL;
    strncpy(table_name, tc->table_name, sizeof(table_name) - 1);
    table_name[sizeof(table_name) - 1] = '\0';
    snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", filename);

    out = fopen(temp_filename, "wb");
    if (!out) return 0;
    setvbuf(out, NULL, _IOFBF, TABLE_FILE_BUFFER_SIZE);

    if (!write_table_header(out, tc)) goto fail;
    for (i = 0; i < tc->record_count; i++) {
        if (!slot_is_active(tc, i)) continue;
        char *row = slot_row(tc, i);
        if (!row || fprintf(out, "%s\n", row) < 0) goto fail;
    }
    if (fflush(out) != 0 || ferror(out)) goto fail;
    if (fclose(out) != 0) {
        remove(temp_filename);
        tc->file = fopen(filename, "r+b");
        return 0;
    }
    if (!replace_table_file(temp_filename, filename)) {
        remove(temp_filename);
        tc->file = fopen(filename, "r+b");
        return 0;
    }
    tc->file = fopen(filename, "r+b");
    if (!tc->file) {
        printf("[warning] table file was rewritten, but could not be reopened for append.\n");
    } else {
        setvbuf(tc->file, NULL, _IOFBF, TABLE_FILE_BUFFER_SIZE);
    }
    clear_delta_log(tc);
    remove_index_snapshot(tc);
    discard_table_cache_for_reload(tc);
    return load_table_contents(tc, table_name, tc->file);

fail:
    fclose(out);
    remove(temp_filename);
    tc->file = fopen(filename, "r+b");
    return 0;
}

static int compact_table_file_for_shutdown(TableCache *tc) {
    char filename[300];
    char temp_filename[320];
    char delta_filename[300];
    FILE *out;
    int i;

    if (!tc || !tc->file) return 1;
    snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", filename);
    get_delta_filename(tc, delta_filename, sizeof(delta_filename));

    out = fopen(temp_filename, "wb");
    if (!out) return 0;
    if (!write_table_header(out, tc)) goto fail;
    for (i = 0; i < tc->record_count; i++) {
        char *row;

        if (!slot_is_active(tc, i)) continue;
        row = slot_row(tc, i);
        if (!row || fprintf(out, "%s\n", row) < 0) goto fail;
    }
    if (fflush(out) != 0 || ferror(out)) goto fail;
    if (fclose(out) != 0) {
        remove(temp_filename);
        return 0;
    }
    out = NULL;

    if (tc->delta_file) {
        if (!close_delta_batch(tc)) {
            remove(temp_filename);
            return 0;
        }
        fclose(tc->delta_file);
        tc->delta_file = NULL;
    }
    if (fflush(tc->file) != 0) {
        remove(temp_filename);
        return 0;
    }
    fclose(tc->file);
    tc->file = NULL;
    if (!replace_table_file(temp_filename, filename)) {
        remove(temp_filename);
        return 0;
    }
    remove(delta_filename);
    remove_index_snapshot(tc);
    tc->delta_batch_open = 0;
    tc->delta_ops_since_compact_check = 0;
    tc->delta_bytes_since_compact = 0;
    return 1;

fail:
    if (out) fclose(out);
    remove(temp_filename);
    return 0;
}

static int rebuild_id_index(TableCache *tc) {
    BPlusTree *new_index;
    BPlusPair *pairs = NULL;
    long next_auto_id = 1;
    int pair_count = 0;
    int sorted = 1;
    int i;

    if (!tc) return 0;
    new_index = bptree_create();
    if (!new_index) return 0;

    if (tc->active_count > 0) {
        pairs = (BPlusPair *)calloc((size_t)tc->active_count, sizeof(BPlusPair));
        if (!pairs) {
            bptree_destroy(new_index);
            return 0;
        }
    }
    for (i = 0; i < tc->record_count; i++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        long id_value;
        char *row = slot_row(tc, i);

        if (!row) continue;
        parse_csv_row(row, fields, row_buf);
        if (get_row_index_key(tc, row, i, &id_value)) {
            if (pair_count > 0 && pairs[pair_count - 1].key >= id_value) sorted = 0;
            pairs[pair_count].key = id_value;
            pairs[pair_count].row_index = i;
            pair_count++;
            if (tc->pk_idx != -1 && id_value >= next_auto_id) next_auto_id = id_value + 1;
        } else {
            free(pairs);
            bptree_destroy(new_index);
            return 0;
        }
    }
    if (!sorted && pair_count > 1) qsort(pairs, (size_t)pair_count, sizeof(BPlusPair), compare_bplus_pair);
    if (!bptree_build_from_sorted(new_index, pairs, pair_count)) {
        free(pairs);
        bptree_destroy(new_index);
        return 0;
    }
    free(pairs);
    bptree_destroy(tc->id_index);
    tc->id_index = new_index;
    if (tc->pk_idx != -1) tc->next_auto_id = next_auto_id;
    return 1;
}

static int ensure_uk_indexes(TableCache *tc) {
    int i;
    for (i = 0; i < tc->uk_count; i++) {
        if (!tc->uk_indexes[i]) {
            tc->uk_indexes[i] = unique_index_create(tc->uk_indices[i]);
            if (!tc->uk_indexes[i]) return 0;
        }
    }
    return 1;
}

static int index_record_uks(TableCache *tc, int row_index) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    char *row;
    int i;

    if (tc->uk_count == 0) return 1;
    if (!ensure_uk_indexes(tc)) return 0;
    row = slot_row(tc, row_index);
    if (!row) return 1;
    parse_csv_row(row, fields, row_buf);
    for (i = 0; i < tc->uk_count; i++) {
        char key[RECORD_SIZE];
        int col_idx = tc->uk_indices[i];
        int result;

        normalize_value(fields[col_idx], key, sizeof(key));
        if (strlen(key) == 0) continue;
        result = unique_index_insert(tc, tc->uk_indexes[i], key, row_index);
        if (result != 1) return 0;
    }
    return 1;
}

static int index_record_uks_from_row(TableCache *tc, const char *row, int row_index) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    int i;

    if (!tc || !row) return 0;
    if (tc->uk_count == 0) return 1;
    if (!ensure_uk_indexes(tc)) return 0;
    parse_csv_row(row, fields, row_buf);
    for (i = 0; i < tc->uk_count; i++) {
        char key[RECORD_SIZE];
        int col_idx = tc->uk_indices[i];
        int result;

        normalize_value(fields[col_idx], key, sizeof(key));
        if (strlen(key) == 0) continue;
        result = unique_index_insert(tc, tc->uk_indexes[i], key, row_index);
        if (result != 1) return 0;
    }
    return 1;
}

static int index_record_single_uk(TableCache *tc, int row_index, int col_idx) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    char key[RECORD_SIZE];
    int uk_slot;
    char *row;

    if (!tc || get_uk_slot(tc, col_idx) == -1) return 1;
    if (!ensure_uk_indexes(tc)) return 0;
    row = slot_row(tc, row_index);
    if (!row) return 0;
    parse_csv_row(row, fields, row_buf);
    normalize_value(fields[col_idx], key, sizeof(key));
    uk_slot = get_uk_slot(tc, col_idx);
    if (uk_slot == -1) return 0;
    return unique_index_insert(tc, tc->uk_indexes[uk_slot], key, row_index);
}

static int remove_record_single_uk(TableCache *tc, const char *row, int col_idx) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    char key[RECORD_SIZE];
    int uk_slot;

    if (!tc || !row || get_uk_slot(tc, col_idx) == -1) return 1;
    if (!ensure_uk_indexes(tc)) return 0;
    parse_csv_row(row, fields, row_buf);
    normalize_value(fields[col_idx], key, sizeof(key));
    if (strlen(key) == 0) return 1;
    uk_slot = get_uk_slot(tc, col_idx);
    if (uk_slot == -1) return 0;
    return bptree_string_delete(tc->uk_indexes[uk_slot]->tree, key);
}

static int remove_record_uk_indexes(TableCache *tc, const char *row) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    int i;

    if (!tc || !row) return 0;
    if (tc->uk_count == 0) return 1;
    if (!ensure_uk_indexes(tc)) return 0;
    parse_csv_row(row, fields, row_buf);
    for (i = 0; i < tc->uk_count; i++) {
        char key[RECORD_SIZE];
        int col_idx = tc->uk_indices[i];

        normalize_value(fields[col_idx], key, sizeof(key));
        if (strlen(key) == 0) continue;
        if (!bptree_string_delete(tc->uk_indexes[i]->tree, key)) return 0;
    }
    return 1;
}

static int build_updated_row(TableCache *tc, const char *row, int set_idx,
                             const char *set_value, char *new_row, size_t new_row_size) {
    const char *field_start;
    const char *p;
    const char *field_end = NULL;
    int current_col = 0;
    int in_quotes = 0;
    char formatted[RECORD_SIZE];
    size_t formatted_len;
    size_t prefix_len;
    size_t suffix_len;

    if (!tc || !row || !new_row || new_row_size == 0) return 0;
    if (set_idx < 0 || set_idx >= tc->col_count) return 0;
    new_row[0] = '\0';

    field_start = row;
    p = row;
    while (1) {
        if (*p == '\'') in_quotes = !in_quotes;
        if ((*p == ',' && !in_quotes) || *p == '\0' || *p == '\r' || *p == '\n') {
            if (current_col == set_idx) {
                field_end = p;
                break;
            }
            if (*p == '\0' || *p == '\r' || *p == '\n') break;
            current_col++;
            field_start = p + 1;
        }
        p++;
    }
    if (!field_end) return 0;

    if (strchr(set_value ? set_value : "", ',')) {
        snprintf(formatted, sizeof(formatted), "'%.*s'",
                 (int)(sizeof(formatted) - 3), set_value ? set_value : "");
    } else {
        snprintf(formatted, sizeof(formatted), "%s", set_value ? set_value : "");
    }
    formatted_len = strlen(formatted);
    prefix_len = (size_t)(field_start - row);
    suffix_len = strlen(field_end);
    if (prefix_len + formatted_len + suffix_len >= new_row_size) return 0;
    memcpy(new_row, row, prefix_len);
    memcpy(new_row + prefix_len, formatted, formatted_len);
    memcpy(new_row + prefix_len + formatted_len, field_end, suffix_len + 1);
    return 1;
}

static int remove_record_indexes(TableCache *tc, const char *row) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    int i;

    if (!tc || !row) return 0;
    parse_csv_row(row, fields, row_buf);
    {
        long id_value;
        if (tc->pk_idx != -1) {
            if (!parse_long_value(fields[tc->pk_idx], &id_value)) return 0;
        } else {
            int i;
            for (i = 0; i < tc->record_count; i++) {
                if (slot_row(tc, i) == row && get_row_index_key(tc, row, i, &id_value)) break;
            }
            if (i == tc->record_count) return 0;
        }
        if (!bptree_delete(tc->id_index, id_value)) return 0;
    }
    if (tc->uk_count == 0) return 1;
    if (!ensure_uk_indexes(tc)) return 0;
    for (i = 0; i < tc->uk_count; i++) {
        char key[RECORD_SIZE];
        int col_idx = tc->uk_indices[i];
        int uk_slot = get_uk_slot(tc, col_idx);

        if (uk_slot == -1) return 0;
        normalize_value(fields[col_idx], key, sizeof(key));
        if (strlen(key) == 0) continue;
        if (!bptree_string_delete(tc->uk_indexes[uk_slot]->tree, key)) return 0;
    }
    return 1;
}

static int restore_record_indexes(TableCache *tc, int slot_id) {
    char *row;
    long id_value;

    if (!tc || !slot_is_active(tc, slot_id)) return 0;
    row = slot_row(tc, slot_id);
    if (!get_row_index_key(tc, row, slot_id, &id_value)) return 0;
    if (bptree_insert(tc->id_index, id_value, slot_id) != 1) return 0;
    return index_record_uks(tc, slot_id);
}

static int rebuild_uk_indexes(TableCache *tc) {
    UniqueIndex *new_indexes[MAX_UKS] = {0};
    BPlusStringPair *pairs[MAX_UKS] = {0};
    int pair_counts[MAX_UKS] = {0};
    int i;
    int row_index;

    if (!tc) return 0;
    for (i = 0; i < tc->uk_count; i++) {
        new_indexes[i] = unique_index_create(tc->uk_indices[i]);
        if (!new_indexes[i]) goto fail;
        if (tc->active_count > 0) {
            pairs[i] = (BPlusStringPair *)calloc((size_t)tc->active_count, sizeof(BPlusStringPair));
            if (!pairs[i]) goto fail;
        }
    }

    for (row_index = 0; row_index < tc->record_count; row_index++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        char *row = slot_row(tc, row_index);

        if (!row) continue;
        parse_csv_row(row, fields, row_buf);
        for (i = 0; i < tc->uk_count; i++) {
            char key[RECORD_SIZE];
            int col_idx = tc->uk_indices[i];

            normalize_value(fields[col_idx], key, sizeof(key));
            if (strlen(key) == 0) continue;
            pairs[i][pair_counts[i]].key = dup_string(key);
            if (!pairs[i][pair_counts[i]].key) goto fail;
            pairs[i][pair_counts[i]].row_index = row_index;
            pair_counts[i]++;
        }
    }

    for (i = 0; i < tc->uk_count; i++) {
        if (pair_counts[i] > 1) {
            qsort(pairs[i], (size_t)pair_counts[i], sizeof(BPlusStringPair), compare_bplus_string_pair);
        }
        if (!bptree_string_build_from_sorted(new_indexes[i]->tree, pairs[i], pair_counts[i])) goto fail;
        for (row_index = 0; row_index < pair_counts[i]; row_index++) free(pairs[i][row_index].key);
        free(pairs[i]);
        pairs[i] = NULL;
        unique_index_destroy(tc->uk_indexes[i]);
        tc->uk_indexes[i] = new_indexes[i];
    }
    return 1;

fail:
    for (i = 0; i < tc->uk_count; i++) {
        if (pairs[i]) {
            int j;
            for (j = 0; j < pair_counts[i]; j++) free(pairs[i][j].key);
            free(pairs[i]);
        }
        unique_index_destroy(new_indexes[i]);
    }
    return 0;
}

typedef struct {
    long size;
    long mtime;
    int exists;
} FileStamp;

static FileStamp get_file_stamp(const char *filename) {
    struct stat st;
    FileStamp stamp = {0, 0, 0};

    if (filename && stat(filename, &st) == 0) {
        stamp.exists = 1;
        stamp.size = (long)st.st_size;
        stamp.mtime = (long)st.st_mtime;
    }
    return stamp;
}

static void get_index_filename(TableCache *tc, char *filename, size_t filename_size) {
    snprintf(filename, filename_size, "%s.idx", tc->table_name);
}

static void remove_index_snapshot(TableCache *tc) {
    char filename[300];

    if (!tc) return;
    get_index_filename(tc, filename, sizeof(filename));
    remove(filename);
    tc->snapshot_loaded = 0;
    tc->snapshot_dirty = 1;
}

typedef struct {
    FILE *out;
    TableCache *tc;
    int count;
    int failed;
} SnapshotVisitContext;

static int count_active_id_pair(long key, int row_index, void *ctx) {
    SnapshotVisitContext *visit = (SnapshotVisitContext *)ctx;
    (void)key;
    if (visit && slot_is_active(visit->tc, row_index)) visit->count++;
    return 1;
}

static int write_active_id_pair(long key, int row_index, void *ctx) {
    SnapshotVisitContext *visit = (SnapshotVisitContext *)ctx;
    if (!visit || !slot_is_active(visit->tc, row_index)) return 1;
    if (fprintf(visit->out, "%ld\t%d\n", key, row_index) < 0) {
        visit->failed = 1;
        return 0;
    }
    return 1;
}

static int count_active_string_pair(const char *key, int row_index, void *ctx) {
    SnapshotVisitContext *visit = (SnapshotVisitContext *)ctx;
    (void)key;
    if (visit && slot_is_active(visit->tc, row_index)) visit->count++;
    return 1;
}

static int write_active_string_pair(const char *key, int row_index, void *ctx) {
    SnapshotVisitContext *visit = (SnapshotVisitContext *)ctx;
    if (!visit || !slot_is_active(visit->tc, row_index)) return 1;
    if (fprintf(visit->out, "%d\t%s\n", row_index, key) < 0) {
        visit->failed = 1;
        return 0;
    }
    return 1;
}

static int write_index_snapshot_pairs(FILE *out, TableCache *tc) {
    SnapshotVisitContext visit;
    int i;

    visit.out = out;
    visit.tc = tc;
    visit.failed = 0;
    if (fprintf(out, "ID %d\n", tc->active_count) < 0) return 0;
    visit.failed = 0;
    if (!bptree_visit_pairs(tc->id_index, write_active_id_pair, &visit) || visit.failed) return 0;

    if (fprintf(out, "UKSECTIONS %d\n", tc->uk_count) < 0) return 0;
    for (i = 0; i < tc->uk_count; i++) {
        visit.count = 0;
        visit.failed = 0;
        if (!tc->uk_indexes[i] || !tc->uk_indexes[i]->tree) return 0;
        if (!bptree_string_visit_pairs(tc->uk_indexes[i]->tree, count_active_string_pair, &visit)) return 0;
        if (fprintf(out, "UK %d %d\n", tc->uk_indices[i], visit.count) < 0) return 0;
        visit.failed = 0;
        if (!bptree_string_visit_pairs(tc->uk_indexes[i]->tree, write_active_string_pair, &visit) ||
            visit.failed) {
            return 0;
        }
    }
    return 1;
}

static int save_index_snapshot(TableCache *tc) {
    char filename[300];
    char temp_filename[320];
    char csv_filename[300];
    char delta_filename[300];
    FILE *out;
    FileStamp csv_stamp;
    FileStamp delta_stamp;
    int i;

    if (!tc || strlen(tc->table_name) == 0 || tc->cache_truncated) {
        remove_index_snapshot(tc);
        return 1;
    }
    if (tc->snapshot_loaded && !tc->snapshot_dirty) return 1;
    if (tc->file && fflush(tc->file) != 0) return 0;
    if (!close_delta_batch(tc)) return 0;

    get_index_filename(tc, filename, sizeof(filename));
    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", filename);
    snprintf(csv_filename, sizeof(csv_filename), "%s.csv", tc->table_name);
    get_delta_filename(tc, delta_filename, sizeof(delta_filename));
    csv_stamp = get_file_stamp(csv_filename);
    delta_stamp = get_file_stamp(delta_filename);

    out = fopen(temp_filename, "wb");
    if (!out) return 0;
    if (fprintf(out, "SQLPROC_IDX_V2\n") < 0) goto fail;
    if (fprintf(out, "TABLE %s\n", tc->table_name) < 0) goto fail;
    if (fprintf(out, "CSV %d %ld %ld\n", csv_stamp.exists, csv_stamp.size, csv_stamp.mtime) < 0) goto fail;
    if (fprintf(out, "DELTA %d %ld %ld\n", delta_stamp.exists, delta_stamp.size, delta_stamp.mtime) < 0) goto fail;
    if (fprintf(out, "SCHEMA %d %d %d", tc->col_count, tc->pk_idx, tc->uk_count) < 0) goto fail;
    for (i = 0; i < tc->uk_count; i++) {
        if (fprintf(out, " %d", tc->uk_indices[i]) < 0) goto fail;
    }
    if (fprintf(out, "\n") < 0) goto fail;
    if (fprintf(out, "ROWS %d %d %d %d %ld %ld\n", tc->record_count, tc->active_count,
                tc->cache_truncated, tc->tail_count, tc->next_auto_id, tc->next_row_id) < 0) {
        goto fail;
    }
    if (fprintf(out, "SLOTS %d\n", tc->record_count) < 0) goto fail;
    for (i = 0; i < tc->record_count; i++) {
        long row_id = tc->row_ids ? tc->row_ids[i] : 0;
        long offset = tc->row_refs ? tc->row_refs[i].offset : -1;
        int active = tc->record_active ? tc->record_active[i] : 0;
        int store = tc->row_refs ? (int)tc->row_refs[i].store : ROW_STORE_NONE;
        if (store == ROW_STORE_MEMORY && delta_stamp.exists) offset = -1;
        if (fprintf(out, "%d\t%d\t%ld\t%d\t%ld\n", i, active, row_id, store, offset) < 0) goto fail;
    }
    if (!write_index_snapshot_pairs(out, tc)) goto fail;
    if (fprintf(out, "END\n") < 0) goto fail;
    if (fflush(out) != 0 || ferror(out)) goto fail;
    if (fclose(out) != 0) {
        remove(temp_filename);
        return 0;
    }
    if (!replace_table_file(temp_filename, filename)) {
        remove(temp_filename);
        return 0;
    }
    INFO_PRINTF("[index] saved B+ tree index snapshot for table '%s'.\n", tc->table_name);
    tc->snapshot_loaded = 1;
    tc->snapshot_dirty = 0;
    return 1;

fail:
    fclose(out);
    remove(temp_filename);
    return 0;
}

static int read_expected_line(FILE *f, char *line, size_t line_size) {
    char *nl;

    if (!fgets(line, (int)line_size, f)) return 0;
    nl = strpbrk(line, "\r\n");
    if (nl) *nl = '\0';
    return 1;
}

static int load_index_snapshot(TableCache *tc) {
    char filename[300];
    char csv_filename[300];
    char delta_filename[300];
    char line[DELTA_LINE_SIZE];
    FILE *f;
    FileStamp csv_stamp;
    FileStamp delta_stamp;
    int csv_exists;
    long csv_size;
    long csv_mtime;
    int delta_exists;
    long delta_size;
    long delta_mtime;
    int col_count;
    int pk_idx;
    int uk_count;
    int uk_indices[MAX_UKS] = {0};
    int record_count;
    int active_count;
    int cache_truncated;
    int tail_count;
    long next_auto_id;
    long next_row_id;
    int id_count;
    int section_count;
    int snapshot_v2 = 0;
    int i;
    BPlusPair *id_pairs = NULL;
    UniqueIndex *new_indexes[MAX_UKS] = {0};

    if (!tc || strlen(tc->table_name) == 0 || tc->cache_truncated) return 0;
    get_index_filename(tc, filename, sizeof(filename));
    f = fopen(filename, "r");
    if (!f) return 0;
    setvbuf(f, NULL, _IOFBF, TABLE_FILE_BUFFER_SIZE);

    snprintf(csv_filename, sizeof(csv_filename), "%s.csv", tc->table_name);
    get_delta_filename(tc, delta_filename, sizeof(delta_filename));
    csv_stamp = get_file_stamp(csv_filename);
    delta_stamp = get_file_stamp(delta_filename);

    if (!read_expected_line(f, line, sizeof(line))) goto fail;
    if (strcmp(line, "SQLPROC_IDX_V2") == 0) snapshot_v2 = 1;
    else if (strcmp(line, "SQLPROC_IDX_V1") != 0) goto fail;
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "TABLE %255s", csv_filename) != 1 ||
        strcmp(csv_filename, tc->table_name) != 0) goto fail;
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "CSV %d %ld %ld", &csv_exists, &csv_size, &csv_mtime) != 3) goto fail;
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "DELTA %d %ld %ld", &delta_exists, &delta_size, &delta_mtime) != 3) goto fail;
    if (csv_exists != csv_stamp.exists || csv_size != csv_stamp.size || csv_mtime != csv_stamp.mtime ||
        delta_exists != delta_stamp.exists || delta_size != delta_stamp.size ||
        delta_mtime != delta_stamp.mtime) {
        goto fail;
    }

    if (!read_expected_line(f, line, sizeof(line))) goto fail;
    {
        char *p = line;
        if (strncmp(p, "SCHEMA ", 7) != 0) goto fail;
        p += 7;
        if (sscanf(p, "%d %d %d", &col_count, &pk_idx, &uk_count) != 3) goto fail;
        for (i = 0; i < 3; i++) {
            while (*p && !isspace((unsigned char)*p)) p++;
            while (isspace((unsigned char)*p)) p++;
        }
        for (i = 0; i < uk_count && i < MAX_UKS; i++) {
            if (sscanf(p, "%d", &uk_indices[i]) != 1) goto fail;
            while (*p && !isspace((unsigned char)*p)) p++;
            while (isspace((unsigned char)*p)) p++;
        }
    }
    if (col_count != tc->col_count || pk_idx != tc->pk_idx || uk_count != tc->uk_count) goto fail;
    for (i = 0; i < uk_count; i++) {
        if (uk_indices[i] != tc->uk_indices[i]) goto fail;
    }

    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "ROWS %d %d %d %d %ld %ld", &record_count, &active_count, &cache_truncated,
               &tail_count, &next_auto_id, &next_row_id) != 6) goto fail;
    if (record_count != tc->record_count || active_count != tc->active_count ||
        cache_truncated != tc->cache_truncated || tail_count != tc->tail_count) goto fail;

    if (snapshot_v2) {
        int slot_count;
        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "SLOTS %d", &slot_count) != 1 ||
            slot_count != tc->record_count) {
            goto fail;
        }
        for (i = 0; i < slot_count; i++) {
            if (!read_expected_line(f, line, sizeof(line))) goto fail;
        }
    }

    if (!read_expected_line(f, line, sizeof(line)) || sscanf(line, "ID %d", &id_count) != 1) goto fail;
    if (id_count != tc->active_count) goto fail;
    if (id_count > 0) {
        id_pairs = (BPlusPair *)calloc((size_t)id_count, sizeof(BPlusPair));
        if (!id_pairs) goto fail;
    }
    for (i = 0; i < id_count; i++) {
        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "%ld\t%d", &id_pairs[i].key, &id_pairs[i].row_index) != 2 ||
            !slot_is_active(tc, id_pairs[i].row_index)) {
            goto fail;
        }
    }

    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "UKSECTIONS %d", &section_count) != 1 ||
        section_count != tc->uk_count) goto fail;
    for (i = 0; i < tc->uk_count; i++) {
        BPlusStringPair *pairs = NULL;
        int col_idx;
        int count;
        int j;

        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "UK %d %d", &col_idx, &count) != 2 ||
            col_idx != tc->uk_indices[i] || count < 0 || count > tc->active_count) {
            goto fail;
        }
        new_indexes[i] = unique_index_create(col_idx);
        if (!new_indexes[i]) goto fail;
        if (count > 0) {
            pairs = (BPlusStringPair *)calloc((size_t)count, sizeof(BPlusStringPair));
            if (!pairs) goto fail;
        }
        for (j = 0; j < count; j++) {
            char *tab;
            int row_index;

            if (!read_expected_line(f, line, sizeof(line))) {
                free(pairs);
                goto fail;
            }
            tab = strchr(line, '\t');
            if (!tab) {
                free(pairs);
                goto fail;
            }
            *tab++ = '\0';
            row_index = atoi(line);
            if (!slot_is_active(tc, row_index)) {
                free(pairs);
                goto fail;
            }
            pairs[j].row_index = row_index;
            pairs[j].key = dup_string(tab);
            if (!pairs[j].key) {
                free(pairs);
                goto fail;
            }
        }
        if (!bptree_string_build_from_sorted(new_indexes[i]->tree, pairs, count)) {
            for (j = 0; j < count; j++) free(pairs[j].key);
            free(pairs);
            goto fail;
        }
        for (j = 0; j < count; j++) free(pairs[j].key);
        free(pairs);
    }
    if (!read_expected_line(f, line, sizeof(line)) || strcmp(line, "END") != 0) goto fail;

    if (!bptree_build_from_sorted(tc->id_index, id_pairs, id_count)) goto fail;
    if (tc->pk_idx == -1) {
        for (i = 0; i < id_count; i++) {
            tc->row_ids[id_pairs[i].row_index] = id_pairs[i].key;
        }
    }
    for (i = 0; i < tc->uk_count; i++) {
        unique_index_destroy(tc->uk_indexes[i]);
        tc->uk_indexes[i] = new_indexes[i];
        new_indexes[i] = NULL;
    }
    tc->next_auto_id = next_auto_id;
    tc->next_row_id = next_row_id;
    free(id_pairs);
    fclose(f);
    INFO_PRINTF("[index] loaded B+ tree index snapshot for table '%s'.\n", tc->table_name);
    tc->snapshot_loaded = 1;
    tc->snapshot_dirty = 0;
    return 1;

fail:
    free(id_pairs);
    for (i = 0; i < MAX_UKS; i++) unique_index_destroy(new_indexes[i]);
    fclose(f);
    return 0;
}

static int load_table_parse_snapshot(TableCache *tc) {
    char filename[300];
    char csv_filename[300];
    char delta_filename[300];
    char line[DELTA_LINE_SIZE];
    FILE *f;
    FileStamp csv_stamp;
    FileStamp delta_stamp;
    int csv_exists;
    long csv_size;
    long csv_mtime;
    int delta_exists;
    long delta_size;
    long delta_mtime;
    int col_count;
    int pk_idx;
    int uk_count;
    int uk_indices[MAX_UKS] = {0};
    int record_count;
    int active_count;
    int cache_truncated;
    int tail_count;
    long next_auto_id;
    long next_row_id;
    int slot_count;
    int id_count;
    int section_count;
    int i;
    int active_seen = 0;
    BPlusPair *id_pairs = NULL;
    UniqueIndex *new_indexes[MAX_UKS] = {0};

    if (!tc || strlen(tc->table_name) == 0) return 0;
    get_index_filename(tc, filename, sizeof(filename));
    f = fopen(filename, "r");
    if (!f) return 0;
    setvbuf(f, NULL, _IOFBF, TABLE_FILE_BUFFER_SIZE);

    snprintf(csv_filename, sizeof(csv_filename), "%s.csv", tc->table_name);
    get_delta_filename(tc, delta_filename, sizeof(delta_filename));
    csv_stamp = get_file_stamp(csv_filename);
    delta_stamp = get_file_stamp(delta_filename);
    if (!read_expected_line(f, line, sizeof(line)) || strcmp(line, "SQLPROC_IDX_V2") != 0) goto fail;
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "TABLE %255s", csv_filename) != 1 ||
        strcmp(csv_filename, tc->table_name) != 0) goto fail;
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "CSV %d %ld %ld", &csv_exists, &csv_size, &csv_mtime) != 3) goto fail;
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "DELTA %d %ld %ld", &delta_exists, &delta_size, &delta_mtime) != 3) goto fail;
    if (csv_exists != csv_stamp.exists || csv_size != csv_stamp.size || csv_mtime != csv_stamp.mtime) {
        goto fail;
    }
    if (!delta_stamp.exists) {
        if (delta_exists != delta_stamp.exists || delta_size != delta_stamp.size ||
            delta_mtime != delta_stamp.mtime) {
            goto fail;
        }
    }

    if (!read_expected_line(f, line, sizeof(line))) goto fail;
    {
        char *p = line;
        if (strncmp(p, "SCHEMA ", 7) != 0) goto fail;
        p += 7;
        if (sscanf(p, "%d %d %d", &col_count, &pk_idx, &uk_count) != 3) goto fail;
        for (i = 0; i < 3; i++) {
            while (*p && !isspace((unsigned char)*p)) p++;
            while (isspace((unsigned char)*p)) p++;
        }
        for (i = 0; i < uk_count && i < MAX_UKS; i++) {
            if (sscanf(p, "%d", &uk_indices[i]) != 1) goto fail;
            while (*p && !isspace((unsigned char)*p)) p++;
            while (isspace((unsigned char)*p)) p++;
        }
    }
    if (col_count != tc->col_count || pk_idx != tc->pk_idx || uk_count != tc->uk_count) goto fail;
    for (i = 0; i < uk_count; i++) {
        if (uk_indices[i] != tc->uk_indices[i]) goto fail;
    }
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "ROWS %d %d %d %d %ld %ld", &record_count, &active_count,
               &cache_truncated, &tail_count, &next_auto_id, &next_row_id) != 6) {
        goto fail;
    }
    if (record_count < 0 || record_count > MAX_RECORDS || active_count < 0 ||
        cache_truncated || tail_count != 0) {
        goto fail;
    }
    if (!ensure_record_capacity(tc, record_count)) goto fail;
    tc->record_count = record_count;
    tc->active_count = 0;
    tc->cache_truncated = 0;
    tc->tail_count = 0;

    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "SLOTS %d", &slot_count) != 1 ||
        slot_count != record_count) {
        goto fail;
    }
    for (i = 0; i < slot_count; i++) {
        int slot_id;
        int active;
        int store;
        long row_id;
        long offset;

        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "%d\t%d\t%ld\t%d\t%ld", &slot_id, &active, &row_id, &store, &offset) != 5 ||
            slot_id != i) {
            goto fail;
        }
        tc->records[i] = NULL;
        tc->row_ids[i] = row_id;
        tc->record_active[i] = active ? 1 : 0;
        tc->row_store[i] = active ? ROW_STORE_CSV : ROW_STORE_NONE;
        tc->row_offsets[i] = active ? offset : 0;
        tc->row_cached[i] = 0;
        tc->row_cache_seq[i] = 0;
        tc->row_refs[i].store = active ? ROW_STORE_CSV : ROW_STORE_NONE;
        tc->row_refs[i].offset = active ? offset : 0;
        if (active) active_seen++;
        else if (!push_free_slot(tc, i)) goto fail;
    }
    if (active_seen != active_count) goto fail;
    tc->active_count = active_seen;

    if (!read_expected_line(f, line, sizeof(line)) || sscanf(line, "ID %d", &id_count) != 1) goto fail;
    if (id_count != active_count) goto fail;
    if (id_count > 0) {
        id_pairs = (BPlusPair *)calloc((size_t)id_count, sizeof(BPlusPair));
        if (!id_pairs) goto fail;
    }
    for (i = 0; i < id_count; i++) {
        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "%ld\t%d", &id_pairs[i].key, &id_pairs[i].row_index) != 2 ||
            !slot_is_active(tc, id_pairs[i].row_index)) {
            goto fail;
        }
    }
    if (!bptree_build_from_sorted(tc->id_index, id_pairs, id_count)) goto fail;

    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "UKSECTIONS %d", &section_count) != 1 ||
        section_count != tc->uk_count) goto fail;
    for (i = 0; i < tc->uk_count; i++) {
        BPlusStringPair *pairs = NULL;
        int col_idx;
        int count;
        int j;

        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "UK %d %d", &col_idx, &count) != 2 ||
            col_idx != tc->uk_indices[i] || count < 0 || count > active_count) {
            goto fail;
        }
        new_indexes[i] = unique_index_create(col_idx);
        if (!new_indexes[i]) goto fail;
        if (count > 0) {
            pairs = (BPlusStringPair *)calloc((size_t)count, sizeof(BPlusStringPair));
            if (!pairs) goto fail;
        }
        for (j = 0; j < count; j++) {
            char *tab;
            int row_index;

            if (!read_expected_line(f, line, sizeof(line))) {
                free(pairs);
                goto fail;
            }
            tab = strchr(line, '\t');
            if (!tab) {
                free(pairs);
                goto fail;
            }
            *tab++ = '\0';
            row_index = atoi(line);
            if (!slot_is_active(tc, row_index)) {
                free(pairs);
                goto fail;
            }
            pairs[j].row_index = row_index;
            pairs[j].key = dup_string(tab);
            if (!pairs[j].key) {
                free(pairs);
                goto fail;
            }
        }
        if (!bptree_string_build_from_sorted(new_indexes[i]->tree, pairs, count)) {
            for (j = 0; j < count; j++) free(pairs[j].key);
            free(pairs);
            goto fail;
        }
        for (j = 0; j < count; j++) free(pairs[j].key);
        free(pairs);
    }
    if (!read_expected_line(f, line, sizeof(line)) || strcmp(line, "END") != 0) goto fail;

    for (i = 0; i < tc->uk_count; i++) {
        unique_index_destroy(tc->uk_indexes[i]);
        tc->uk_indexes[i] = new_indexes[i];
        new_indexes[i] = NULL;
    }
    tc->next_auto_id = next_auto_id;
    tc->next_row_id = next_row_id;
    free(id_pairs);
    fclose(f);
    if (fseek(tc->file, 0, SEEK_END) == 0) tc->append_offset = ftell(tc->file);
    INFO_PRINTF("[index] loaded CSV parse/index snapshot for table '%s'.\n", tc->table_name);
    tc->snapshot_loaded = 1;
    tc->snapshot_dirty = 0;
    return 1;

fail:
    free(id_pairs);
    for (i = 0; i < MAX_UKS; i++) unique_index_destroy(new_indexes[i]);
    fclose(f);
    return 0;
}

static int load_table_contents(TableCache *tc, const char *name, FILE *f) {
    char header[RECORD_SIZE];
    char line[RECORD_SIZE];
    long line_number = 1;
    long file_next_auto_id = 1;
    int has_delta_log;

    reset_table_cache(tc);
    if (!tc->id_index) return 0;
    strncpy(tc->table_name, name, sizeof(tc->table_name) - 1);
    tc->file = f;
    touch_table(tc);
    has_delta_log = delta_log_exists(tc);

    if (fgets(header, sizeof(header), f)) {
        if ((unsigned char)header[0] == 0xEF &&
            (unsigned char)header[1] == 0xBB &&
            (unsigned char)header[2] == 0xBF) {
            memmove(header, header + 3, strlen(header + 3) + 1);
        }
        char *token = strtok(header, ",\r\n");
        int idx = 0;

        while (token && idx < MAX_COLS) {
            char *paren = strchr(token, '(');
            if (paren) {
                int len = (int)(paren - token);
                if (len >= (int)sizeof(tc->cols[idx].name)) len = (int)sizeof(tc->cols[idx].name) - 1;
                strncpy(tc->cols[idx].name, token, (size_t)len);
                tc->cols[idx].name[len] = '\0';

                if (strstr(paren, "(PK)")) {
                    tc->cols[idx].type = COL_PK;
                    tc->pk_idx = idx;
                } else if (strstr(paren, "(UK)")) {
                    tc->cols[idx].type = COL_UK;
                    if (tc->uk_count < MAX_UKS) tc->uk_indices[tc->uk_count++] = idx;
                } else if (strstr(paren, "(NN)")) {
                    tc->cols[idx].type = COL_NN;
                } else {
                    tc->cols[idx].type = COL_NORMAL;
                }
            } else {
                strncpy(tc->cols[idx].name, token, sizeof(tc->cols[idx].name) - 1);
                tc->cols[idx].name[sizeof(tc->cols[idx].name) - 1] = '\0';
                tc->cols[idx].type = COL_NORMAL;
            }
            token = strtok(NULL, ",\r\n");
            idx++;
        }
        tc->col_count = idx;
        if (!ensure_uk_indexes(tc)) return 0;
    }

    if (load_table_parse_snapshot(tc)) {
        if (delta_log_exists(tc) && !replay_delta_log(tc)) {
            printf("[error] failed to replay delta log while loading '%s'.\n", name);
            return 0;
        }
        return 1;
    }

    while (1) {
        char *nl;
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        long id_value = 0;
        long row_id;
        long row_offset = ftell(f);
        size_t line_len;
        int loaded_slot = -1;

        if (row_offset < 0) return 0;
        if (!fgets(line, sizeof(line), f)) break;
        nl = strpbrk(line, "\r\n");
        line_len = strlen(line);

        line_number++;
        if (!nl && line_len == sizeof(line) - 1 && !feof(f)) {
            printf("[error] row too long while loading '%s' at line %ld (max %d bytes).\n",
                   name, line_number, RECORD_SIZE - 1);
            return 0;
        }

        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;

        parse_csv_row(line, fields, row_buf);
        if (tc->pk_idx != -1 && fields[tc->pk_idx] && !parse_long_value(fields[tc->pk_idx], &id_value)) {
            printf("[error] invalid PK value while loading '%s': %s\n", name, fields[tc->pk_idx]);
            return 0;
        }
        if (tc->pk_idx != -1 && id_value >= file_next_auto_id) file_next_auto_id = id_value + 1;
        row_id = tc->next_row_id++;

        if (tc->active_count < MAX_RECORDS) {
            if (!append_record_raw_memory(tc, line, row_id, row_offset, &loaded_slot)) {
                printf("[error] failed to load row into memory.\n");
                return 0;
            }
            if (!has_delta_log) {
                if (bptree_insert(tc->id_index, tc->pk_idx != -1 ? id_value : row_id, loaded_slot) != 1 ||
                    !index_record_uks_from_row(tc, line, loaded_slot)) {
                    printf("[error] failed to build indexes while loading '%s'.\n", name);
                    return 0;
                }
            }
        } else {
            if (!tc->cache_truncated) tc->uncached_start_offset = row_offset;
            if (!append_tail_pk_offset(tc, tc->pk_idx != -1 ? id_value : row_id, row_offset)) {
                printf("[error] failed to build tail offset index while loading '%s'.\n", name);
                return 0;
            }
            tc->cache_truncated = 1;
        }
    }
    if (has_delta_log && tc->active_count == 0 && tc->tail_count == 0) {
        if (!clear_delta_log(tc)) {
            printf("[error] failed to clear stale delta log for empty table '%s'.\n", name);
            return 0;
        }
        remove_index_snapshot(tc);
        has_delta_log = 0;
    }

    if (!has_delta_log) {
        if (tc->active_count == 0) {
            remove_index_snapshot(tc);
        } else if (!load_index_snapshot(tc)) {
            if (!save_index_snapshot(tc)) {
                INFO_PRINTF("[warning] failed to save index snapshot for table '%s'.\n", name);
            }
        }
        if (file_next_auto_id > tc->next_auto_id) tc->next_auto_id = file_next_auto_id;
    } else {
        if (!replay_delta_log(tc)) {
            printf("[error] failed to replay delta log while loading '%s'.\n", name);
            return 0;
        }
        if (!load_index_snapshot(tc)) {
            if (!rebuild_id_index(tc) || !rebuild_uk_indexes(tc)) {
                printf("[error] failed to bulk-build indexes while loading '%s'.\n", name);
                return 0;
            }
            if (file_next_auto_id > tc->next_auto_id) tc->next_auto_id = file_next_auto_id;
            if (!save_index_snapshot(tc)) {
                INFO_PRINTF("[warning] failed to save index snapshot for table '%s'.\n", name);
            }
        }
    }
    if (tc->cache_truncated) {
        INFO_PRINTF("[notice] table '%s' exceeded memory cache limit (%d rows). Extra rows stay on disk; PK equality can use tail offset index, other predicates scan the tail.\n",
               name, MAX_RECORDS);
    }
    if (fseek(f, 0, SEEK_END) == 0) {
        tc->append_offset = ftell(f);
    }
    return 1;
}

TableCache *get_table(const char *name) {
    char filename[300];
    FILE *f;
    TableCache *tc;
    int i;

    for (i = 0; i < open_table_count; i++) {
        if (strcmp(open_tables[i].table_name, name) == 0) {
            touch_table(&open_tables[i]);
            return &open_tables[i];
        }
    }

    snprintf(filename, sizeof(filename), "%s.csv", name);
    f = fopen(filename, "r+b");
    if (!f) {
        printf("[notice] '%s.csv' does not exist.\n", name);
        return NULL;
    }
    setvbuf(f, NULL, _IOFBF, TABLE_FILE_BUFFER_SIZE);

    if (open_table_count < MAX_TABLES) {
        tc = &open_tables[open_table_count++];
    } else {
        int evict_idx = find_lru_table_index();
        tc = &open_tables[evict_idx];
        free_table_storage(tc);
    }

    if (!load_table_contents(tc, name, f)) {
        free_table_storage(tc);
        if (tc == &open_tables[open_table_count - 1]) open_table_count--;
        return NULL;
    }
    return tc;
}

static int reload_table_cache(TableCache *tc) {
    char table_name[256];
    char filename[300];
    FILE *f;

    if (!tc) return 0;
    strncpy(table_name, tc->table_name, sizeof(table_name) - 1);
    table_name[sizeof(table_name) - 1] = '\0';
    snprintf(filename, sizeof(filename), "%s.csv", table_name);

    free_table_storage(tc);
    f = fopen(filename, "r+b");
    if (!f) return 0;
    setvbuf(f, NULL, _IOFBF, TABLE_FILE_BUFFER_SIZE);
    return load_table_contents(tc, table_name, f);
}

static int build_insert_row(TableCache *tc, char *vals[MAX_COLS], int val_count, long *id_value, char *new_line, size_t line_size) {
    int i;
    size_t offset = 0;
    int has_auto_id = 0;

    *id_value = 0;
    if (val_count > tc->col_count) {
        printf("[error] INSERT failed: too many values for table '%s'.\n", tc->table_name);
        return 0;
    }
    if (tc->pk_idx != -1) {
        char *pk_val = (tc->pk_idx < val_count) ? vals[tc->pk_idx] : NULL;
        if (val_count == tc->col_count - 1 && tc->pk_idx == 0) has_auto_id = 1;
        if (!pk_val || strlen(pk_val) == 0 || compare_value(pk_val, "NULL")) has_auto_id = 1;

        if (has_auto_id) *id_value = tc->next_auto_id;
        else if (!parse_long_value(pk_val, id_value)) {
            printf("[error] INSERT failed: PK column '%s' must be an integer.\n", tc->cols[tc->pk_idx].name);
            return 0;
        }

        {
            int existing_row;
            if (bptree_search(tc->id_index, *id_value, &existing_row) &&
                slot_is_active(tc, existing_row)) {
                printf("[error] INSERT failed: duplicate PK value %ld.\n", *id_value);
                return 0;
            }
        }
    }

    for (i = 0; i < tc->col_count; i++) {
        const char *source;
        char normalized[RECORD_SIZE];
        char formatted[RECORD_SIZE];
        int source_index = i;
        int w;

        if (has_auto_id && i == tc->pk_idx) {
            snprintf(normalized, sizeof(normalized), "%ld", *id_value);
            source = normalized;
        } else {
            if (has_auto_id && tc->pk_idx == 0) source_index = i - 1;
            source = (source_index >= 0 && source_index < val_count && vals[source_index]) ? vals[source_index] : "";
            strncpy(normalized, source, sizeof(normalized) - 1);
            normalized[sizeof(normalized) - 1] = '\0';
            trim_and_unquote(normalized);
            source = normalized;
        }

        if (tc->cols[i].type == COL_NN && strlen(source) == 0) {
            printf("[error] INSERT failed: column '%s' violates NN constraint.\n", tc->cols[i].name);
            return 0;
        }
        if (i == tc->pk_idx && strlen(source) == 0) {
            printf("[error] INSERT failed: PK column '%s' is empty.\n", tc->cols[i].name);
            return 0;
        }
        if (tc->cols[i].type == COL_UK && strlen(source) > 0) {
            int uk_slot = get_uk_slot(tc, i);
            if (uk_slot == -1 || !ensure_uk_indexes(tc)) {
                printf("[error] INSERT failed: UK index is not available.\n");
                return 0;
            }
            if (unique_index_find(tc, tc->uk_indexes[uk_slot], source, NULL)) {
                printf("[error] INSERT failed: '%s' violates UK constraint.\n", source);
                return 0;
            }
        }

        if (strchr(source, ',')) {
            snprintf(formatted, sizeof(formatted), "'%.*s'", (int)(sizeof(formatted) - 3), source);
            source = formatted;
        }
        w = snprintf(new_line + offset, line_size - offset, "%s%s", source, (i < tc->col_count - 1) ? "," : "");
        if (w < 0 || (size_t)w >= line_size - offset) {
            printf("[error] INSERT failed: row is too long.\n");
            return 0;
        }
        offset += (size_t)w;
    }
    return 1;
}

static int append_csv_field(char *row, size_t row_size, size_t *offset, const char *value, int is_last) {
    char formatted[RECORD_SIZE];
    const char *source = value ? value : "";
    int w;

    if (strchr(source, ',')) {
        snprintf(formatted, sizeof(formatted), "'%.*s'", (int)(sizeof(formatted) - 3), source);
        source = formatted;
    }
    w = snprintf(row + *offset, row_size - *offset, "%s%s", source, is_last ? "" : ",");
    if (w < 0 || (size_t)w >= row_size - *offset) return 0;
    *offset += (size_t)w;
    return 1;
}

static int validate_file_duplicates_for_uncached_insert(TableCache *tc, const char *new_line) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    int i;

    if (!tc || !new_line || (!tc->cache_truncated && tc->active_count < MAX_RECORDS)) return 1;
    parse_csv_row(new_line, fields, row_buf);

    if (tc->pk_idx != -1 && table_file_has_value(tc, tc->pk_idx, fields[tc->pk_idx])) {
        printf("[error] INSERT failed: duplicate PK value %s.\n", fields[tc->pk_idx]);
        return 0;
    }
    for (i = 0; i < tc->uk_count; i++) {
        int col_idx = tc->uk_indices[i];
        if (fields[col_idx] && strlen(fields[col_idx]) > 0 &&
            table_file_has_value(tc, col_idx, fields[col_idx])) {
            printf("[error] INSERT failed: '%s' violates UK constraint.\n", fields[col_idx]);
            return 0;
        }
    }
    return 1;
}

static int insert_row_data(TableCache *tc, const char *row_data, int flush_now, long *inserted_id) {
    char buffer[RECORD_SIZE];
    char *vals[MAX_COLS] = {0};
    char new_line[RECORD_SIZE] = "";
    int val_count = 0;
    long id_value = 0;
    int inserted_slot = -1;

    if (!tc) return 0;
    parse_csv_row(row_data, vals, buffer);
    while (val_count < MAX_COLS && vals[val_count]) val_count++;

    if (!build_insert_row(tc, vals, val_count, &id_value, new_line, sizeof(new_line))) return 0;

    if (!tc->cache_truncated && tc->active_count >= MAX_RECORDS && delta_log_exists(tc)) {
        if (!rewrite_file(tc)) {
            printf("[error] INSERT failed: could not compact pending delta log before tail append.\n");
            return 0;
        }
    }
    if (!validate_file_duplicates_for_uncached_insert(tc, new_line)) return 0;

    if (!tc->cache_truncated && (tc->active_count < MAX_RECORDS || tc->free_count > 0)) {
        long append_offset;

        if (tc->append_offset < 0) {
            if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_END) != 0) {
                printf("[error] INSERT failed: could not find append position.\n");
                return 0;
            }
            tc->append_offset = ftell(tc->file);
        }
        append_offset = tc->append_offset;
        if (append_offset < 0) {
            printf("[error] INSERT failed: could not find append position.\n");
            return 0;
        }
        if (!append_record_csv_indexed(tc, new_line, id_value, append_offset, &inserted_slot)) {
            printf("[error] INSERT failed: could not update B+ tree index or RowRef store.\n");
            return 0;
        }
        if (!append_record_file(tc, new_line, flush_now)) {
            if (inserted_slot < 0 || !deactivate_slot(tc, inserted_slot, 1) ||
                !rebuild_id_index(tc) || !rebuild_uk_indexes(tc)) {
                printf("[error] INSERT rollback failed: indexes may be stale.\n");
            }
            printf("[error] INSERT failed: could not append to table file.\n");
            return 0;
        }
    } else {
        long append_offset;

        if (tc->append_offset < 0) {
            if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_END) != 0) {
                printf("[error] INSERT failed: could not find append position.\n");
                return 0;
            }
            tc->append_offset = ftell(tc->file);
        }
        append_offset = tc->append_offset;
        if (append_offset < 0) {
            printf("[error] INSERT failed: could not find append position.\n");
            return 0;
        }
        if (!ensure_tail_index_capacity(tc, tc->tail_count + 1)) {
            printf("[error] INSERT failed: could not grow tail offset index.\n");
            return 0;
        }
        if (!append_record_file(tc, new_line, flush_now)) {
            printf("[error] INSERT failed: could not append to table file.\n");
            return 0;
        }
        if (!tc->cache_truncated) tc->uncached_start_offset = append_offset;
        if (!append_tail_pk_offset(tc, tc->pk_idx != -1 ? id_value : tc->next_row_id++, append_offset)) {
            printf("[error] INSERT failed: could not update tail offset index.\n");
            return 0;
        }
        tc->cache_truncated = 1;
        if (id_value >= tc->next_auto_id) tc->next_auto_id = id_value + 1;
        INFO_PRINTF("[notice] INSERT appended to CSV only; memory cache limit is %d rows, so later lookup may use slower file scan.\n",
               MAX_RECORDS);
    }
    if (inserted_id) *inserted_id = id_value;
    tc->snapshot_dirty = 1;
    return 1;
}

void execute_insert(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    long id_value = 0;

    if (!tc) return;
    if (!insert_row_data(tc, stmt->row_data, 0, &id_value)) return;
    INFO_PRINTF("[ok] INSERT completed. id=%ld\n", id_value);
}

typedef struct {
    int select_idx[MAX_COLS];
    int select_count;
    int select_all;
    int emit_results;
    int emit_traces;
    int matched_rows;
} SelectExecContext;

typedef struct {
    TableCache *tc;
    Statement *stmt;
    SelectExecContext *exec;
} RangePrintContext;

static void emit_selected_row(const char *row, SelectExecContext *exec) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    int j;

    if (!row || !exec) return;
    exec->matched_rows++;
    if (!exec->emit_results) return;

    if (exec->select_all) {
        printf("%s\n", row);
        return;
    }
    parse_csv_row(row, fields, row_buf);
    for (j = 0; j < exec->select_count; j++) {
        if (j > 0) printf(",");
        printf("%s", fields[exec->select_idx[j]] ? fields[exec->select_idx[j]] : "");
    }
    printf("\n");
}

static int condition_column_index(TableCache *tc, const WhereCondition *cond) {
    if (!cond || cond->type == WHERE_NONE) return -1;
    return get_col_idx(tc, cond->col);
}

static int compare_range_value(TableCache *tc, int col_idx, const char *field,
                               const WhereCondition *cond) {
    long row_key;
    long start_key;
    long end_key;
    char row_text[RECORD_SIZE];
    char start_text[RECORD_SIZE];
    char end_text[RECORD_SIZE];

    if (!tc || !field || !cond || col_idx < 0) return 0;
    if (col_idx == tc->pk_idx) {
        if (!parse_long_value(field, &row_key) ||
            !parse_long_value(cond->val, &start_key) ||
            !parse_long_value(cond->end_val, &end_key)) {
            return 0;
        }
        return row_key >= start_key && row_key <= end_key;
    }
    normalize_value(field, row_text, sizeof(row_text));
    normalize_value(cond->val, start_text, sizeof(start_text));
    normalize_value(cond->end_val, end_text, sizeof(end_text));
    return strcmp(row_text, start_text) >= 0 && strcmp(row_text, end_text) <= 0;
}

static int row_matches_statement(TableCache *tc, Statement *stmt, const char *row) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};

    if (!tc || !stmt || !row) return 0;
    if (stmt->where_count == 0) return 1;
    if (stmt->where_count == 1 && stmt->where_conditions[0].type == WHERE_EQ) {
        int col_idx = condition_column_index(tc, &stmt->where_conditions[0]);
        if (col_idx < 0 || col_idx >= tc->col_count) return 0;
        return row_field_equals(tc, row, col_idx, stmt->where_conditions[0].val);
    }
    parse_csv_row(row, fields, row_buf);
    return row_fields_match_statement(tc, stmt, fields);
}

static int row_fields_match_statement(TableCache *tc, Statement *stmt, char *fields[MAX_COLS]) {
    int i;

    if (!tc || !stmt || !fields) return 0;
    if (stmt->where_count == 0) return 1;
    for (i = 0; i < stmt->where_count; i++) {
        WhereCondition *cond = &stmt->where_conditions[i];
        int col_idx = condition_column_index(tc, cond);
        if (col_idx < 0 || col_idx >= tc->col_count) return 0;
        if (cond->type == WHERE_EQ) {
            if (!compare_value(fields[col_idx], cond->val)) return 0;
        } else if (cond->type == WHERE_BETWEEN) {
            if (!compare_range_value(tc, col_idx, fields[col_idx], cond)) return 0;
        } else {
            return 0;
        }
    }
    return 1;
}

static int validate_where_columns(TableCache *tc, Statement *stmt, const char *op_name) {
    int i;

    if (!tc || !stmt) return 0;
    for (i = 0; i < stmt->where_count; i++) {
        if (condition_column_index(tc, &stmt->where_conditions[i]) == -1) {
            printf("[error] %s failed: WHERE column '%s' does not exist.\n",
                   op_name, stmt->where_conditions[i].col);
            return 0;
        }
    }
    return 1;
}

static int choose_index_condition(TableCache *tc, Statement *stmt, int allow_range,
                                  int *condition_index, int *where_idx) {
    int i;
    int best = -1;
    int best_score = 0;
    int best_col = -1;

    if (condition_index) *condition_index = -1;
    if (where_idx) *where_idx = -1;
    if (!tc || !stmt) return 0;

    for (i = 0; i < stmt->where_count; i++) {
        WhereCondition *cond = &stmt->where_conditions[i];
        int col_idx = condition_column_index(tc, cond);
        int score = 0;

        if (col_idx == -1) continue;
        if (cond->type == WHERE_EQ && col_idx == tc->pk_idx) score = 100;
        else if (cond->type == WHERE_EQ && get_uk_slot(tc, col_idx) != -1) score = 90;
        else if (allow_range && cond->type == WHERE_BETWEEN && col_idx == tc->pk_idx) score = 80;
        else if (allow_range && cond->type == WHERE_BETWEEN && get_uk_slot(tc, col_idx) != -1) score = 70;

        if (score > best_score) {
            best_score = score;
            best = i;
            best_col = col_idx;
        }
    }
    if (best == -1) return 0;
    if (condition_index) *condition_index = best;
    if (where_idx) *where_idx = best_col;
    return 1;
}
static int print_range_row_visitor(long key, int row_index, void *ctx) {
    RangePrintContext *range_ctx = (RangePrintContext *)ctx;
    (void)key;

    if (!range_ctx || !range_ctx->tc || !range_ctx->exec) return 0;
    if (!slot_is_active(range_ctx->tc, row_index)) return 1;
    {
        char *row = slot_row(range_ctx->tc, row_index);
        if (row && row_matches_statement(range_ctx->tc, range_ctx->stmt, row)) {
            emit_selected_row(row, range_ctx->exec);
        }
    }
    return 1;
}

static int print_string_range_row_visitor(const char *key, int row_index, void *ctx) {
    RangePrintContext *range_ctx = (RangePrintContext *)ctx;
    (void)key;

    if (!range_ctx || !range_ctx->tc || !range_ctx->exec) return 0;
    if (!slot_is_active(range_ctx->tc, row_index)) return 1;
    {
        char *row = slot_row(range_ctx->tc, row_index);
        if (row && row_matches_statement(range_ctx->tc, range_ctx->stmt, row)) {
            emit_selected_row(row, range_ctx->exec);
        }
    }
    return 1;
}

static void execute_select_file_scan(TableCache *tc, long start_offset, Statement *stmt,
                                     SelectExecContext *exec) {
    char line[RECORD_SIZE];

    if (!tc || !tc->file || !stmt || !exec) return;
    if (fflush(tc->file) != 0) {
        printf("[error] SELECT failed: could not scan table file.\n");
        return;
    }
    if (start_offset > 0) {
        if (fseek(tc->file, start_offset, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not scan uncached table rows.\n");
            return;
        }
        if (exec->emit_traces) printf("[scan] uncached CSV tail scan from offset %ld\n", start_offset);
    } else {
        if (fseek(tc->file, 0, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not scan table file.\n");
            return;
        }
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return;
        }
        if (exec->emit_traces) printf("[scan] full CSV scan\n");
    }

    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        size_t line_len = strlen(line);

        if (!nl && line_len == sizeof(line) - 1 && !feof(tc->file)) {
            printf("[error] row too long while scanning '%s' (max %d bytes).\n",
                   tc->table_name, RECORD_SIZE - 1);
            fseek(tc->file, 0, SEEK_END);
            return;
        }
        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        if (!row_matches_statement(tc, stmt, line)) continue;
        emit_selected_row(line, exec);
    }
    fseek(tc->file, 0, SEEK_END);
}

static int print_tail_pk_offset_row(TableCache *tc, long offset, long key,
                                    Statement *stmt, SelectExecContext *exec) {
    char line[RECORD_SIZE];
    char *nl;
    size_t line_len;
    long row_id;

    if (!tc || !tc->file || offset < 0 || !exec) return 0;
    if (fflush(tc->file) != 0 || fseek(tc->file, offset, SEEK_SET) != 0) return 0;
    if (!fgets(line, sizeof(line), tc->file)) {
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }
    nl = strpbrk(line, "\r\n");
    line_len = strlen(line);
    if (!nl && line_len == sizeof(line) - 1 && !feof(tc->file)) {
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }
    if (nl) *nl = '\0';
    if (!get_row_pk_value(tc, line, &row_id) || row_id != key) {
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }
    if (!row_matches_statement(tc, stmt, line)) {
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }
    if (exec->emit_traces) INFO_PRINTF("[index] tail PK offset lookup\n");
    emit_selected_row(line, exec);
    fseek(tc->file, 0, SEEK_END);
    return 1;
}

static void execute_select_file_range_scan(TableCache *tc, long start_offset, Statement *stmt,
                                           int where_idx, SelectExecContext *exec,
                                           long start_key, long end_key) {
    char line[RECORD_SIZE];

    if (!tc || !tc->file || !stmt || !exec || start_key > end_key) return;
    if (fflush(tc->file) != 0) {
        printf("[error] SELECT failed: could not scan table file.\n");
        return;
    }
    if (start_offset > 0) {
        if (fseek(tc->file, start_offset, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not seek uncached CSV tail.\n");
            return;
        }
        if (exec->emit_traces) printf("[scan] uncached CSV tail range scan from offset %ld\n", start_offset);
    } else {
        if (fseek(tc->file, 0, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not scan table file.\n");
            return;
        }
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return;
        }
        if (exec->emit_traces) printf("[scan] full CSV range scan\n");
    }

    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        size_t line_len = strlen(line);
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        long row_key;

        if (!nl && line_len == sizeof(line) - 1 && !feof(tc->file)) {
            printf("[error] row too long while scanning '%s' (max %d bytes).\n",
                   tc->table_name, RECORD_SIZE - 1);
            fseek(tc->file, 0, SEEK_END);
            return;
        }
        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        parse_csv_row(line, fields, row_buf);
        if (!parse_long_value(fields[where_idx], &row_key)) continue;
        if (row_key >= start_key && row_key <= end_key &&
            row_fields_match_statement(tc, stmt, fields)) {
            emit_selected_row(line, exec);
        }
    }
    fseek(tc->file, 0, SEEK_END);
}

static void execute_select_file_string_range_scan(TableCache *tc, long start_offset, Statement *stmt,
                                                  int where_idx, SelectExecContext *exec,
                                                  const char *start_key, const char *end_key) {
    char line[RECORD_SIZE];

    if (!tc || !tc->file || !stmt || !exec || !start_key || !end_key ||
        strcmp(start_key, end_key) > 0) {
        return;
    }
    if (fflush(tc->file) != 0) {
        printf("[error] SELECT failed: could not scan table file.\n");
        return;
    }
    if (start_offset > 0) {
        if (fseek(tc->file, start_offset, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not seek uncached CSV tail.\n");
            return;
        }
        if (exec->emit_traces) printf("[scan] uncached CSV tail string range scan from offset %ld\n", start_offset);
    } else {
        if (fseek(tc->file, 0, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not scan table file.\n");
            return;
        }
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return;
        }
        if (exec->emit_traces) printf("[scan] full CSV string range scan\n");
    }

    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        size_t line_len = strlen(line);
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        char key[RECORD_SIZE];

        if (!nl && line_len == sizeof(line) - 1 && !feof(tc->file)) {
            printf("[error] row too long while scanning '%s' (max %d bytes).\n",
                   tc->table_name, RECORD_SIZE - 1);
            fseek(tc->file, 0, SEEK_END);
            return;
        }
        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        parse_csv_row(line, fields, row_buf);
        normalize_value(fields[where_idx], key, sizeof(key));
        if (strcmp(key, start_key) >= 0 && strcmp(key, end_key) <= 0 &&
            row_fields_match_statement(tc, stmt, fields)) {
            emit_selected_row(line, exec);
        }
    }
    fseek(tc->file, 0, SEEK_END);
}

static int execute_select_internal(Statement *stmt, int emit_results, int emit_traces, int *matched_rows) {
    TableCache *tc = get_table(stmt->table_name);
    int index_cond = -1;
    int index_col = -1;
    SelectExecContext exec;
    int i;

    if (matched_rows) *matched_rows = 0;
    if (!tc) return 0;
    if (!validate_where_columns(tc, stmt, "SELECT")) return 0;
    memset(&exec, 0, sizeof(exec));
    exec.select_all = stmt->select_all;
    exec.emit_results = emit_results;
    exec.emit_traces = emit_traces;

    if (!stmt->select_all) {
        for (i = 0; i < stmt->select_col_count; i++) {
            int idx = get_col_idx(tc, stmt->select_cols[i]);
            if (idx == -1) {
                printf("[error] SELECT failed: unknown column '%s'.\n", stmt->select_cols[i]);
                return 0;
            }
            exec.select_idx[i] = idx;
        }
        exec.select_count = stmt->select_col_count;
    }

    if (exec.emit_results) printf("\n--- [SELECT RESULT] table=%s ---\n", tc->table_name);
    choose_index_condition(tc, stmt, 1, &index_cond, &index_col);

    if (index_cond != -1 && stmt->where_conditions[index_cond].type == WHERE_BETWEEN) {
        WhereCondition *cond = &stmt->where_conditions[index_cond];
        long start_key;
        long end_key;
        RangePrintContext range_ctx;

        range_ctx.tc = tc;
        range_ctx.stmt = stmt;
        range_ctx.exec = &exec;

        if (index_col == tc->pk_idx) {
            if (!parse_long_value(cond->val, &start_key) ||
                !parse_long_value(cond->end_val, &end_key)) {
                printf("[error] SELECT failed: BETWEEN bounds must be integers for PK range search.\n");
                return 0;
            }
            if (exec.emit_traces) INFO_PRINTF("[index] B+ tree id range lookup\n");
            if (!bptree_range_search(tc->id_index, start_key, end_key, print_range_row_visitor, &range_ctx)) {
                printf("[error] SELECT failed: B+ tree range scan failed.\n");
                return 0;
            }
            if (tc->cache_truncated) {
                execute_select_file_range_scan(tc, tc->uncached_start_offset, stmt, index_col, &exec,
                                               start_key, end_key);
            }
            if (matched_rows) *matched_rows = exec.matched_rows;
            return 1;
        }

        if (get_uk_slot(tc, index_col) != -1) {
            int uk_slot = get_uk_slot(tc, index_col);
            char start_text[RECORD_SIZE];
            char end_text[RECORD_SIZE];

            if (uk_slot == -1 || !ensure_uk_indexes(tc)) {
                printf("[error] SELECT failed: UK index is not available.\n");
                return 0;
            }
            normalize_value(cond->val, start_text, sizeof(start_text));
            normalize_value(cond->end_val, end_text, sizeof(end_text));
            if (exec.emit_traces) INFO_PRINTF("[index] UK B+ tree range lookup on column '%s'\n", cond->col);
            if (!bptree_string_range_search(tc->uk_indexes[uk_slot]->tree, start_text, end_text,
                                            print_string_range_row_visitor, &range_ctx)) {
                printf("[error] SELECT failed: UK B+ tree range scan failed.\n");
                return 0;
            }
            if (tc->cache_truncated) {
                execute_select_file_string_range_scan(tc, tc->uncached_start_offset, stmt, index_col, &exec,
                                                      start_text, end_text);
            }
            if (matched_rows) *matched_rows = exec.matched_rows;
            return 1;
        }

        printf("[error] SELECT failed: BETWEEN uses PK or UK B+ tree indexes only.\n");
        return 0;
    }

    if (index_cond != -1 && stmt->where_conditions[index_cond].type == WHERE_EQ &&
        index_col == tc->pk_idx && index_col != -1) {
        WhereCondition *cond = &stmt->where_conditions[index_cond];
        long key;
        int row_index;
        if (!parse_long_value(cond->val, &key)) {
            printf("[error] SELECT failed: id condition must be an integer.\n");
            return 0;
        }
        if (exec.emit_traces) INFO_PRINTF("[index] B+ tree id lookup\n");
        if (bptree_search(tc->id_index, key, &row_index)) {
            if (!exec.emit_results && stmt->where_count == 1) {
                if (matched_rows) *matched_rows = 1;
                return 1;
            }
            char *row = slot_row(tc, row_index);
            if (row && row_matches_statement(tc, stmt, row)) emit_selected_row(row, &exec);
            if (matched_rows) *matched_rows = exec.matched_rows;
            return 1;
        }
        if (tc->cache_truncated) {
            long tail_offset;
            if (find_tail_pk_offset(tc, key, &tail_offset)) {
                if (!exec.emit_results && stmt->where_count == 1) {
                    if (matched_rows) *matched_rows = 1;
                    return 1;
                }
                if (print_tail_pk_offset_row(tc, tail_offset, key, stmt, &exec)) {
                    if (matched_rows) *matched_rows = exec.matched_rows;
                    return 1;
                }
            }
            execute_select_file_scan(tc, tc->uncached_start_offset, stmt, &exec);
        }
        if (matched_rows) *matched_rows = exec.matched_rows;
        return 1;
    }

    if (index_cond != -1 && stmt->where_conditions[index_cond].type == WHERE_EQ &&
        index_col != -1 && get_uk_slot(tc, index_col) != -1) {
        WhereCondition *cond = &stmt->where_conditions[index_cond];
        int row_index;
        if (exec.emit_traces) INFO_PRINTF("[index] UK B+ tree lookup on column '%s'\n", cond->col);
        if (find_uk_row(tc, index_col, cond->val, &row_index)) {
            if (!exec.emit_results && stmt->where_count == 1) {
                if (matched_rows) *matched_rows = 1;
                return 1;
            }
            char *row = slot_row(tc, row_index);
            if (row && row_matches_statement(tc, stmt, row)) emit_selected_row(row, &exec);
            if (matched_rows) *matched_rows = exec.matched_rows;
            return 1;
        }
        if (tc->cache_truncated) {
            execute_select_file_scan(tc, tc->uncached_start_offset, stmt, &exec);
        }
        if (matched_rows) *matched_rows = exec.matched_rows;
        return 1;
    }

    if (stmt->where_count == 1 && exec.emit_traces) {
        printf("[scan] linear scan on column '%s'\n", stmt->where_conditions[0].col);
    } else if (stmt->where_count > 1 && exec.emit_traces) {
        printf("[scan] linear scan with %d WHERE condition(s)\n", stmt->where_count);
    }
    for (i = 0; i < tc->record_count; i++) {
        int owned = 0;
        char *row = slot_row_scan(tc, i, &owned);
        if (!row) continue;
        {
            char row_buf[RECORD_SIZE];
            char *fields[MAX_COLS] = {0};
            parse_csv_row(row, fields, row_buf);
            if (!row_fields_match_statement(tc, stmt, fields)) {
                if (owned) free(row);
                continue;
            }
        }
        emit_selected_row(row, &exec);
        if (owned) free(row);
    }
    if (tc->cache_truncated) {
        execute_select_file_scan(tc, tc->uncached_start_offset, stmt, &exec);
    }
    if (matched_rows) *matched_rows = exec.matched_rows;
    return 1;
}

void execute_select(Statement *stmt) {
    int emit = g_executor_quiet ? 0 : 1;
    (void)execute_select_internal(stmt, emit, emit, NULL);
}

static int rewrite_truncated_update(TableCache *tc, Statement *stmt,
                                    int set_idx, const char *set_value) {
    char filename[300];
    char temp_filename[320];
    char line[RECORD_SIZE];
    FILE *out;
    int count = 0;
    int target_count = 0;
    int uk_conflict = 0;

    if (!tc || !tc->file) return 0;
    if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_SET) != 0) return 0;
    if (!fgets(line, sizeof(line), tc->file)) return 0;

    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        int matched;

        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        parse_csv_row(line, fields, row_buf);
        matched = row_matches_statement(tc, stmt, line);
        if (matched) target_count++;
        if (!matched && tc->cols[set_idx].type == COL_UK && strlen(set_value) > 0 &&
            compare_value(fields[set_idx], set_value)) {
            uk_conflict = 1;
        }
    }
    if (target_count == 0) {
        INFO_PRINTF("[notice] no rows matched UPDATE condition.\n");
        fseek(tc->file, 0, SEEK_END);
        return 1;
    }
    if (tc->cols[set_idx].type == COL_UK && target_count > 1) {
        printf("[error] UPDATE failed: multiple rows would share one UK value.\n");
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }
    if (uk_conflict) {
        printf("[error] UPDATE failed: '%s' violates UK constraint.\n", set_value);
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }

    snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", filename);
    out = fopen(temp_filename, "wb");
    if (!out) return 0;
    if (!write_table_header(out, tc)) goto fail;

    if (fseek(tc->file, 0, SEEK_SET) != 0) goto fail;
    if (!fgets(line, sizeof(line), tc->file)) goto fail;
    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        int matched;

        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        parse_csv_row(line, fields, row_buf);
        matched = row_matches_statement(tc, stmt, line);
        if (matched) {
            char new_row[RECORD_SIZE] = "";
            size_t offset = 0;
            int j;

            for (j = 0; j < tc->col_count; j++) {
                const char *val = (j == set_idx) ? set_value : (fields[j] ? fields[j] : "");
                if (!append_csv_field(new_row, sizeof(new_row), &offset, val, j == tc->col_count - 1)) goto fail;
            }
            if (fprintf(out, "%s\n", new_row) < 0) goto fail;
            count++;
        } else {
            if (fprintf(out, "%s\n", line) < 0) goto fail;
        }
    }
    if (fflush(out) != 0 || ferror(out)) goto fail;
    if (fclose(out) != 0) {
        remove(temp_filename);
        return 0;
    }
    fclose(tc->file);
    tc->file = NULL;
    if (!replace_table_file(temp_filename, filename)) {
        remove(temp_filename);
        tc->file = fopen(filename, "r+b");
        return 0;
    }
    if (!reload_table_cache(tc)) return 0;
    INFO_PRINTF("[ok] UPDATE completed with CSV scan fallback. rows=%d\n", count);
    return 1;

fail:
    fclose(out);
    remove(temp_filename);
    fseek(tc->file, 0, SEEK_END);
    return 0;
}

static int rewrite_truncated_delete(TableCache *tc, Statement *stmt) {
    char filename[300];
    char temp_filename[320];
    char line[RECORD_SIZE];
    FILE *out;
    int count = 0;

    if (!tc || !tc->file) return 0;
    snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", filename);
    out = fopen(temp_filename, "wb");
    if (!out) return 0;
    if (!write_table_header(out, tc)) goto fail;

    if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_SET) != 0) goto fail;
    if (!fgets(line, sizeof(line), tc->file)) goto fail;
    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        int matched;

        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        matched = row_matches_statement(tc, stmt, line);
        if (matched) {
            count++;
            continue;
        }
        if (fprintf(out, "%s\n", line) < 0) goto fail;
    }
    if (fflush(out) != 0 || ferror(out)) goto fail;
    if (fclose(out) != 0) {
        remove(temp_filename);
        return 0;
    }
    if (count == 0) {
        remove(temp_filename);
        INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
        fseek(tc->file, 0, SEEK_END);
        return 1;
    }
    fclose(tc->file);
    tc->file = NULL;
    if (!replace_table_file(temp_filename, filename)) {
        remove(temp_filename);
        tc->file = fopen(filename, "r+b");
        return 0;
    }
    if (!reload_table_cache(tc)) return 0;
    INFO_PRINTF("[ok] DELETE completed with CSV scan fallback. rows=%d\n", count);
    return 1;

fail:
    fclose(out);
    remove(temp_filename);
    fseek(tc->file, 0, SEEK_END);
    return 0;
}

static int execute_update_single_row(TableCache *tc, Statement *stmt, int where_idx,
                                     const WhereCondition *lookup_cond, int set_idx, const char *set_value, int uses_pk_lookup,
                                     int uses_uk_lookup, int rebuild_uk_needed) {
    int target_row = -1;
    char *old_record;
    char *new_copy;
    char new_row[RECORD_SIZE];
    long pk_key = 0;
    int has_pk_key = 0;

    if (uses_pk_lookup) {
        INFO_PRINTF("[index] B+ tree id lookup for UPDATE\n");
        if (!find_pk_row(tc, lookup_cond->val, &target_row)) return 0;
        has_pk_key = parse_long_value(lookup_cond->val, &pk_key);
    } else if (uses_uk_lookup) {
        INFO_PRINTF("[index] UK B+ tree lookup for UPDATE on column '%s'\n", lookup_cond->col);
        if (!find_uk_row(tc, where_idx, lookup_cond->val, &target_row)) return 0;
    } else {
        return -1;
    }

    old_record = slot_row(tc, target_row);
    if (!old_record) {
        printf("[error] UPDATE failed: target row could not be loaded.\n");
        return -1;
    }
    if (stmt->where_count != 1 && !row_matches_statement(tc, stmt, old_record)) return 0;
    if (rebuild_uk_needed) {
        int found_row = -1;
        int uk_slot = get_uk_slot(tc, set_idx);
        if (uk_slot == -1 || !ensure_uk_indexes(tc)) {
            printf("[error] UPDATE failed: UK index is not available.\n");
            return -1;
        }
        if (strlen(set_value) > 0 &&
            unique_index_find(tc, tc->uk_indexes[uk_slot], set_value, &found_row) &&
            found_row != target_row) {
            printf("[error] UPDATE failed: '%s' violates UK constraint.\n", set_value);
            return -1;
        }
    }
    if (!build_updated_row(tc, old_record, set_idx, set_value, new_row, sizeof(new_row))) {
        printf("[error] UPDATE failed: rebuilt row is too long.\n");
        return -1;
    }
    new_copy = dup_string(new_row);
    if (!new_copy) {
        printf("[error] UPDATE failed: out of memory.\n");
        return -1;
    }
    if (rebuild_uk_needed && !remove_record_single_uk(tc, old_record, set_idx)) {
        free(new_copy);
        printf("[error] UPDATE failed: UK index update failed; memory restored.\n");
        return -1;
    }
    if (tc->row_cached[target_row] && tc->cached_record_count > 0) {
        tc->cached_record_count--;
    }
    tc->records[target_row] = new_copy;
    tc->row_store[target_row] = ROW_STORE_MEMORY;
    tc->row_offsets[target_row] = -1;
    if (tc->row_refs) {
        tc->row_refs[target_row].store = ROW_STORE_MEMORY;
        tc->row_refs[target_row].offset = -1;
    }
    tc->row_cached[target_row] = 0;
    tc->row_cache_seq[target_row] = 0;
    if (rebuild_uk_needed && !index_record_single_uk(tc, target_row, set_idx)) {
        free(tc->records[target_row]);
        tc->records[target_row] = old_record;
        rebuild_uk_indexes(tc);
        printf("[error] UPDATE failed: UK index update failed; memory restored.\n");
        return -1;
    }
    if (can_use_delta_log(tc)) {
        int delta_ok = has_pk_key ?
            append_delta_update_key(tc, pk_key, tc->records[target_row]) :
            append_delta_update_slot(tc, target_row, tc->records[target_row]);
        if (!delta_ok) {
            free(tc->records[target_row]);
            tc->records[target_row] = old_record;
            if (rebuild_uk_needed) rebuild_uk_indexes(tc);
            printf("[error] UPDATE failed: delta log append failed; memory restored.\n");
            return -1;
        }
        INFO_PRINTF("[delta] UPDATE persisted through append-only delta log.\n");
        if (!maybe_compact_delta_log(tc)) {
            printf("[warning] UPDATE completed, but delta compaction failed.\n");
        }
    } else if (!rewrite_file(tc)) {
        free(tc->records[target_row]);
        tc->records[target_row] = old_record;
        if (rebuild_uk_needed) rebuild_uk_indexes(tc);
        printf("[error] UPDATE warning: memory changed but file rewrite failed.\n");
        return -1;
    }
    free(old_record);
    INFO_PRINTF("[ok] UPDATE completed. rows=1\n");
    return 1;
}

void execute_update(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    int where_idx = -1;
    int index_cond = -1;
    int index_col = -1;
    int set_idx;
    char set_value[256];
    int *match_flags;
    char **old_records;
    int target_count = 0;
    int uses_pk_lookup = 0;
    int uses_uk_lookup = 0;
    int target_row = -1;
    int rebuild_uk_needed;
    int i;

    if (!tc) return;
    if (!validate_where_columns(tc, stmt, "UPDATE")) return;
    if (stmt->where_count == 0) {
        printf("[error] UPDATE failed: WHERE condition is required.\n");
        return;
    }
    choose_index_condition(tc, stmt, 0, &index_cond, &index_col);
    where_idx = index_col != -1 ? index_col : condition_column_index(tc, &stmt->where_conditions[0]);
    set_idx = get_col_idx(tc, stmt->set_col);
    if (where_idx == -1 || set_idx == -1) {
        printf("[error] UPDATE failed: WHERE or SET column does not exist.\n");
        return;
    }
    if (set_idx == tc->pk_idx) {
        printf("[error] UPDATE failed: PK column cannot be changed.\n");
        return;
    }

    strncpy(set_value, stmt->set_val, sizeof(set_value) - 1);
    set_value[sizeof(set_value) - 1] = '\0';
    trim_and_unquote(set_value);
    if (tc->cols[set_idx].type == COL_NN && strlen(set_value) == 0) {
        printf("[error] UPDATE failed: column '%s' violates NN constraint.\n", tc->cols[set_idx].name);
        return;
    }
    if (tc->cache_truncated) {
        if (!rewrite_truncated_update(tc, stmt, set_idx, set_value)) {
            printf("[error] UPDATE failed while using CSV scan fallback.\n");
        }
        return;
    }
    rebuild_uk_needed = (tc->cols[set_idx].type == COL_UK);
    uses_pk_lookup = (index_cond != -1 && index_col == tc->pk_idx &&
                      stmt->where_conditions[index_cond].type == WHERE_EQ);
    uses_uk_lookup = (index_cond != -1 && !uses_pk_lookup && get_uk_slot(tc, index_col) != -1 &&
                      stmt->where_conditions[index_cond].type == WHERE_EQ);

    if (uses_pk_lookup || uses_uk_lookup) {
        int result = execute_update_single_row(tc, stmt, index_col, &stmt->where_conditions[index_cond],
                                               set_idx, set_value,
                                               uses_pk_lookup, uses_uk_lookup, rebuild_uk_needed);
        if (result == 0) INFO_PRINTF("[notice] no rows matched UPDATE condition.\n");
        return;
    }

    match_flags = (int *)calloc((size_t)tc->record_count, sizeof(int));
    if (!match_flags) {
        printf("[error] UPDATE failed: out of memory.\n");
        return;
    }
    old_records = (char **)calloc((size_t)tc->record_count, sizeof(char *));
    if (!old_records) {
        printf("[error] UPDATE failed: out of memory.\n");
        free(match_flags);
        return;
    }

    {
        for (i = 0; i < tc->record_count; i++) {
            char *row = slot_row(tc, i);
            if (!row) continue;
            if (row_matches_statement(tc, stmt, row)) {
                match_flags[i] = 1;
                target_count++;
            }
        }
    }
    if (target_count == 0) {
        free(old_records);
        free(match_flags);
        INFO_PRINTF("[notice] no rows matched UPDATE condition.\n");
        return;
    }

    if (rebuild_uk_needed) {
        int found_row = -1;
        int uk_slot = get_uk_slot(tc, set_idx);
        if (target_count > 1) {
            printf("[error] UPDATE failed: multiple rows would share one UK value.\n");
            free(old_records);
            free(match_flags);
            return;
        }
        if (uk_slot == -1 || !ensure_uk_indexes(tc)) {
            printf("[error] UPDATE failed: UK index is not available.\n");
            free(old_records);
            free(match_flags);
            return;
        }
        if (strlen(set_value) > 0 &&
            unique_index_find(tc, tc->uk_indexes[uk_slot], set_value, &found_row) &&
            (found_row < 0 || !match_flags[found_row])) {
            printf("[error] UPDATE failed: '%s' violates UK constraint.\n", set_value);
            free(old_records);
            free(match_flags);
            return;
        }
    }

    int count = 0;
    for (i = 0; i < tc->record_count; i++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        char new_row[RECORD_SIZE] = "";
        size_t offset = 0;
        int j;
        char *row;

        if (!match_flags[i]) continue;
        row = slot_row(tc, i);
        if (!row) continue;
        parse_csv_row(row, fields, row_buf);
        for (j = 0; j < tc->col_count; j++) {
            const char *val = (j == set_idx) ? set_value : (fields[j] ? fields[j] : "");
            if (!append_csv_field(new_row, sizeof(new_row), &offset, val, j == tc->col_count - 1)) break;
        }
        if (j != tc->col_count) {
            rollback_updated_records(tc, old_records);
            printf("[error] UPDATE failed: rebuilt row is too long.\n");
            free(old_records);
            free(match_flags);
            return;
        }
        char *new_copy = dup_string(new_row);
        if (!new_copy) {
            rollback_updated_records(tc, old_records);
            printf("[error] UPDATE failed: out of memory.\n");
            free(old_records);
            free(match_flags);
            return;
        }
        if (tc->row_cached[i] && tc->cached_record_count > 0) {
            tc->cached_record_count--;
        }
        old_records[i] = tc->records[i];
        tc->records[i] = new_copy;
        tc->row_store[i] = ROW_STORE_MEMORY;
        tc->row_offsets[i] = -1;
        if (tc->row_refs) {
            tc->row_refs[i].store = ROW_STORE_MEMORY;
            tc->row_refs[i].offset = -1;
        }
        tc->row_cached[i] = 0;
        tc->row_cache_seq[i] = 0;
        count++;
    }

    free(match_flags);
    if (count > 0) {
        if (rebuild_uk_needed) {
            for (i = 0; i < tc->record_count; i++) {
                if (!old_records[i]) continue;
                if (!remove_record_single_uk(tc, old_records[i], set_idx) ||
                    !index_record_single_uk(tc, i, set_idx)) {
                    rollback_updated_records(tc, old_records);
                    rebuild_uk_indexes(tc);
                    free(old_records);
                    printf("[error] UPDATE failed: UK index update failed; memory restored.\n");
                    return;
                }
            }
        }
        if (can_use_delta_log(tc)) {
            if (!append_delta_updates(tc, old_records)) {
                rollback_updated_records(tc, old_records);
                if (rebuild_uk_needed) rebuild_uk_indexes(tc);
                free(old_records);
                printf("[error] UPDATE failed: delta log append failed; memory restored.\n");
                return;
            }
            INFO_PRINTF("[delta] UPDATE persisted through append-only delta log.\n");
            if (!maybe_compact_delta_log(tc)) {
                printf("[warning] UPDATE completed, but delta compaction failed.\n");
            }
        } else if (!rewrite_file(tc)) {
            rollback_updated_records(tc, old_records);
            if (rebuild_uk_needed) rebuild_uk_indexes(tc);
            free(old_records);
            printf("[error] UPDATE warning: memory changed but file rewrite failed.\n");
            return;
        }
        for (i = 0; i < tc->record_count; i++) free(old_records[i]);
        free(old_records);
        INFO_PRINTF("[ok] UPDATE completed. rows=%d\n", count);
    } else {
        free(old_records);
        INFO_PRINTF("[notice] no rows matched UPDATE condition.\n");
    }
}

void execute_delete(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    int where_idx = -1;
    int index_cond = -1;
    int index_col = -1;
    int count = 0;
    int read_idx;
    int old_count;
    char **old_records;
    int *delete_flags;
    int *removed_index_flags;
    int uses_pk_lookup = 0;
    int uses_uk_lookup = 0;
    int target_row = -1;

    if (!tc) return;
    if (!validate_where_columns(tc, stmt, "DELETE")) return;
    if (stmt->where_count == 0) {
        printf("[error] DELETE failed: WHERE condition is required.\n");
        return;
    }
    choose_index_condition(tc, stmt, 0, &index_cond, &index_col);
    where_idx = index_col != -1 ? index_col : condition_column_index(tc, &stmt->where_conditions[0]);
    if (tc->cache_truncated) {
        if (!rewrite_truncated_delete(tc, stmt)) {
            printf("[error] DELETE failed while using CSV scan fallback.\n");
        }
        return;
    }

    old_count = tc->record_count;
    uses_pk_lookup = (index_cond != -1 && index_col == tc->pk_idx &&
                      stmt->where_conditions[index_cond].type == WHERE_EQ);
    uses_uk_lookup = (index_cond != -1 && !uses_pk_lookup && get_uk_slot(tc, index_col) != -1 &&
                      stmt->where_conditions[index_cond].type == WHERE_EQ);
    if (old_count == 0) {
        INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
        return;
    }
    if (uses_pk_lookup || uses_uk_lookup) {
        char *old_record;
        long pk_key = 0;
        int has_pk_key = 0;

        if (uses_pk_lookup) {
            INFO_PRINTF("[index] B+ tree id lookup for DELETE\n");
            has_pk_key = parse_long_value(stmt->where_conditions[index_cond].val, &pk_key);
            if (!find_pk_row(tc, stmt->where_conditions[index_cond].val, &target_row)) {
                INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
                return;
            }
        } else {
            INFO_PRINTF("[index] UK B+ tree lookup for DELETE on column '%s'\n",
                        stmt->where_conditions[index_cond].col);
            if (!find_uk_row(tc, index_col, stmt->where_conditions[index_cond].val, &target_row)) {
                INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
                return;
            }
        }
        old_record = slot_row(tc, target_row);
        if (!old_record) {
            printf("[error] DELETE failed: target row could not be loaded.\n");
            return;
        }
        if (uses_pk_lookup && stmt->where_count == 1 && has_pk_key) {
            int pk_matches = 1;
            if (tc->pk_idx != -1) {
                pk_matches = row_field_equals(tc, old_record, tc->pk_idx,
                                              stmt->where_conditions[index_cond].val);
            } else if (tc->row_ids && tc->row_ids[target_row] != pk_key) {
                pk_matches = 0;
            }
            if (!pk_matches) {
                bptree_delete(tc->id_index, pk_key);
                INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
                return;
            }
        }
        if (!(uses_pk_lookup && stmt->where_count == 1 && has_pk_key) &&
            !row_matches_statement(tc, stmt, old_record)) {
            if (uses_pk_lookup && stmt->where_count == 1) {
                long stale_key;
                if (parse_long_value(stmt->where_conditions[index_cond].val, &stale_key)) {
                    bptree_delete(tc->id_index, stale_key);
                }
            } else if (uses_uk_lookup && stmt->where_count == 1) {
                int stale_uk_slot = get_uk_slot(tc, index_col);
                char stale_key[RECORD_SIZE];

                normalize_value(stmt->where_conditions[index_cond].val,
                                stale_key, sizeof(stale_key));
                if (stale_uk_slot != -1 && strlen(stale_key) > 0) {
                    bptree_string_delete(tc->uk_indexes[stale_uk_slot]->tree, stale_key);
                }
            }
            INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
            return;
        }
        if (uses_pk_lookup && stmt->where_count == 1 && has_pk_key) {
            if (!bptree_delete(tc->id_index, pk_key) ||
                !remove_record_uk_indexes(tc, old_record)) {
                printf("[error] DELETE failed: index removal failed; memory unchanged.\n");
                return;
            }
        } else if (!remove_record_indexes(tc, old_record)) {
            printf("[error] DELETE failed: index removal failed; memory unchanged.\n");
            return;
        }
        tc->record_active[target_row] = 0;
        tc->active_count--;
        if (can_use_delta_log(tc)) {
            int delta_ok = (uses_pk_lookup && stmt->where_count == 1 && has_pk_key) ?
                append_delta_delete_key(tc, pk_key) :
                append_delta_delete_one(tc, old_record);
            if (!delta_ok) {
                tc->record_active[target_row] = 1;
                tc->active_count++;
                restore_record_indexes(tc, target_row);
                printf("[error] DELETE failed: delta log append failed; memory restored.\n");
                return;
            }
            INFO_PRINTF("[delta] DELETE persisted through append-only delta log.\n");
            if (!maybe_compact_delta_log(tc)) {
                printf("[warning] DELETE completed, but delta compaction failed.\n");
            }
        } else if (!rewrite_file(tc)) {
            tc->record_active[target_row] = 1;
            tc->active_count++;
            restore_record_indexes(tc, target_row);
            printf("[error] DELETE warning: memory changed but file rewrite failed.\n");
            return;
        }
        free(tc->records[target_row]);
        tc->records[target_row] = NULL;
        if (tc->row_cached[target_row] && tc->cached_record_count > 0) tc->cached_record_count--;
        tc->row_cached[target_row] = 0;
        tc->row_cache_seq[target_row] = 0;
        tc->row_store[target_row] = ROW_STORE_NONE;
        tc->row_offsets[target_row] = 0;
        if (tc->row_refs) {
            tc->row_refs[target_row].store = ROW_STORE_NONE;
            tc->row_refs[target_row].offset = 0;
        }
        push_free_slot(tc, target_row);
        INFO_PRINTF("[ok] DELETE completed. rows=1\n");
        return;
    }
    old_records = (char **)malloc((size_t)old_count * sizeof(char *));
    delete_flags = (int *)calloc((size_t)old_count, sizeof(int));
    removed_index_flags = (int *)calloc((size_t)old_count, sizeof(int));
    if (!old_records || !delete_flags || !removed_index_flags) {
        free(old_records);
        free(delete_flags);
        free(removed_index_flags);
        printf("[error] DELETE failed: out of memory.\n");
        return;
    }
    for (read_idx = 0; read_idx < old_count; read_idx++) {
        old_records[read_idx] = slot_is_active(tc, read_idx) ? slot_row(tc, read_idx) : NULL;
    }

    {
        for (read_idx = 0; read_idx < tc->record_count; read_idx++) {
            int matched;
            char *row = slot_row(tc, read_idx);

            if (!row) continue;
            matched = row_matches_statement(tc, stmt, row);
            if (matched) {
                delete_flags[read_idx] = 1;
                count++;
            }
        }
    }

    if (count > 0) {
        for (read_idx = 0; read_idx < tc->record_count; read_idx++) {
            if (delete_flags[read_idx] && tc->record_active[read_idx]) {
                if (!remove_record_indexes(tc, old_records[read_idx])) {
                    int rollback_idx;
                    for (rollback_idx = 0; rollback_idx < read_idx; rollback_idx++) {
                        if (removed_index_flags[rollback_idx]) restore_record_indexes(tc, rollback_idx);
                    }
                    free(old_records);
                    free(delete_flags);
                    free(removed_index_flags);
                    printf("[error] DELETE failed: index removal failed; memory unchanged.\n");
                    return;
                }
                removed_index_flags[read_idx] = 1;
            }
        }
        for (read_idx = 0; read_idx < tc->record_count; read_idx++) {
            if (delete_flags[read_idx] && tc->record_active[read_idx]) {
                tc->record_active[read_idx] = 0;
                tc->active_count--;
            }
        }
        if (can_use_delta_log(tc)) {
            if (!append_delta_deletes(tc, old_records, delete_flags, old_count)) {
                for (read_idx = 0; read_idx < old_count; read_idx++) {
                    if (delete_flags[read_idx]) {
                        tc->record_active[read_idx] = 1;
                    }
                    if (removed_index_flags[read_idx]) restore_record_indexes(tc, read_idx);
                }
                tc->active_count += count;
                free(old_records);
                free(delete_flags);
                free(removed_index_flags);
                printf("[error] DELETE failed: delta log append failed; memory restored.\n");
                return;
            }
            INFO_PRINTF("[delta] DELETE persisted through append-only delta log.\n");
            if (!maybe_compact_delta_log(tc)) {
                printf("[warning] DELETE completed, but delta compaction failed.\n");
            }
        } else if (!rewrite_file(tc)) {
            for (read_idx = 0; read_idx < old_count; read_idx++) {
                if (delete_flags[read_idx]) {
                    tc->record_active[read_idx] = 1;
                }
                if (removed_index_flags[read_idx]) restore_record_indexes(tc, read_idx);
            }
            tc->active_count += count;
            free(old_records);
            free(delete_flags);
            free(removed_index_flags);
            printf("[error] DELETE warning: memory changed but file rewrite failed.\n");
            return;
        }
        for (read_idx = 0; read_idx < old_count; read_idx++) {
            if (delete_flags[read_idx]) {
                free(tc->records[read_idx]);
                tc->records[read_idx] = NULL;
                if (tc->row_cached[read_idx] && tc->cached_record_count > 0) tc->cached_record_count--;
                tc->row_cached[read_idx] = 0;
                tc->row_cache_seq[read_idx] = 0;
                tc->row_store[read_idx] = ROW_STORE_NONE;
                tc->row_offsets[read_idx] = 0;
                if (tc->row_refs) {
                    tc->row_refs[read_idx].store = ROW_STORE_NONE;
                    tc->row_refs[read_idx].offset = 0;
                }
                push_free_slot(tc, read_idx);
            }
        }
        free(old_records);
        free(delete_flags);
        free(removed_index_flags);
        INFO_PRINTF("[ok] DELETE completed. rows=%d\n", count);
    } else {
        free(old_records);
        free(delete_flags);
        free(removed_index_flags);
        INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
    }
}

static double current_seconds(void) {
#if defined(_WIN32)
    static LARGE_INTEGER frequency;
    LARGE_INTEGER counter;
    if (frequency.QuadPart == 0) QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&counter);
    return (double)counter.QuadPart / (double)frequency.QuadPart;
#else
    return (double)clock() / CLOCKS_PER_SEC;
#endif
}

static void init_eq_select_statement(Statement *stmt, const char *table_name, const char *where_col) {
    if (!stmt) return;
    memset(stmt, 0, sizeof(*stmt));
    stmt->type = STMT_SELECT;
    stmt->select_all = 1;
    stmt->where_count = 1;
    stmt->where_type = WHERE_EQ;
    strncpy(stmt->table_name, table_name ? table_name : "", sizeof(stmt->table_name) - 1);
    stmt->table_name[sizeof(stmt->table_name) - 1] = '\0';
    strncpy(stmt->where_col, where_col ? where_col : "", sizeof(stmt->where_col) - 1);
    stmt->where_col[sizeof(stmt->where_col) - 1] = '\0';
    stmt->where_conditions[0].type = WHERE_EQ;
    strncpy(stmt->where_conditions[0].col, stmt->where_col, sizeof(stmt->where_conditions[0].col) - 1);
    stmt->where_conditions[0].col[sizeof(stmt->where_conditions[0].col) - 1] = '\0';
}

static void set_eq_select_value(Statement *stmt, const char *value) {
    if (!stmt) return;
    strncpy(stmt->where_val, value ? value : "", sizeof(stmt->where_val) - 1);
    stmt->where_val[sizeof(stmt->where_val) - 1] = '\0';
    if (stmt->where_count > 0) {
        strncpy(stmt->where_conditions[0].val, stmt->where_val,
                sizeof(stmt->where_conditions[0].val) - 1);
        stmt->where_conditions[0].val[sizeof(stmt->where_conditions[0].val) - 1] = '\0';
        stmt->where_conditions[0].end_val[0] = '\0';
    }
}

static int ensure_jungle_benchmark_artifacts_absent(void) {
    const char *artifacts[] = {
        JUNGLE_BENCHMARK_CSV,
        "jungle_benchmark_users.delta",
        "jungle_benchmark_users.idx"
    };
    int i;

    for (i = 0; i < (int)(sizeof(artifacts) / sizeof(artifacts[0])); i++) {
        if (!path_exists(artifacts[i])) continue;
        printf("[safe-stop] jungle benchmark artifact already exists: %s\n", artifacts[i]);
        printf("[notice] No files were deleted. Remove or rename the artifact manually, then rerun --benchmark-jungle.\n");
        return 0;
    }
    return 1;
}

void run_jungle_benchmark(int record_count) {
    FILE *f;
    TableCache *tc;
    Statement stmt;
    int i;
    int matched_rows;
    int matched_checks = 0;
    const int id_query_count = 100000;
    const int email_query_count = 100000;
    const int phone_query_count = 100000;
    const int linear_query_count = 30;
    double start;
    double end;
    double insert_time;
    double id_time;
    double email_time;
    double phone_time;
    double linear_time;

    record_count = clamp_record_count(record_count <= 0 ? 1000000 : record_count, 1000000);
    close_all_tables();
    open_table_count = 0;

    if (!ensure_jungle_benchmark_artifacts_absent()) return;

    f = fopen(JUNGLE_BENCHMARK_CSV, "wb");
    if (!f) {
        printf("[error] jungle benchmark table file could not be created.\n");
        return;
    }
    if (fputs(JUNGLE_BENCHMARK_HEADER, f) == EOF || fclose(f) != 0) {
        printf("[error] jungle benchmark table header could not be written.\n");
        return;
    }

    tc = get_table(JUNGLE_BENCHMARK_TABLE);
    if (!tc) return;

    start = current_seconds();
    for (i = 1; i <= record_count; i++) {
        char row_data[512];

        build_jungle_row_data(i, row_data, sizeof(row_data));
        if (!insert_row_data(tc, row_data, 0, NULL)) {
            printf("[error] jungle benchmark insert failed at row %d.\n", i);
            return;
        }
    }
    if (fflush(tc->file) != 0 || ferror(tc->file)) {
        printf("[error] jungle benchmark insert flush failed.\n");
        return;
    }
    end = current_seconds();
    insert_time = end - start;

    printf("\n--- [JUNGLE SQL-PATH BENCHMARK] ---\n");
    printf("dataset theme: jungle applicants 2026 spring\n");
    printf("table file: %s\n", JUNGLE_BENCHMARK_CSV);
    printf("inserted records through INSERT path: %d (%.6f sec)\n", record_count, insert_time);

    init_eq_select_statement(&stmt, JUNGLE_BENCHMARK_TABLE, "id");
    start = current_seconds();
    for (i = 0; i < id_query_count; i++) {
        long key = (long)((i * 7919) % record_count) + 1;
        char target[32];

        snprintf(target, sizeof(target), "%ld", key);
        set_eq_select_value(&stmt, target);
        if (!execute_select_internal(&stmt, 0, 0, &matched_rows)) return;
        if (matched_rows <= 0) {
            printf("[error] jungle benchmark ID lookup returned no rows for key %ld.\n", key);
            return;
        }
        matched_checks++;
    }
    end = current_seconds();
    id_time = end - start;

    init_eq_select_statement(&stmt, JUNGLE_BENCHMARK_TABLE, "email");
    start = current_seconds();
    for (i = 0; i < email_query_count; i++) {
        char target[64];

        build_jungle_email(((i * 7919) % record_count) + 1, target, sizeof(target));
        set_eq_select_value(&stmt, target);
        if (!execute_select_internal(&stmt, 0, 0, &matched_rows)) return;
        if (matched_rows <= 0) {
            printf("[error] jungle benchmark email lookup returned no rows for '%s'.\n", target);
            return;
        }
        matched_checks++;
    }
    end = current_seconds();
    email_time = end - start;

    init_eq_select_statement(&stmt, JUNGLE_BENCHMARK_TABLE, "phone");
    start = current_seconds();
    for (i = 0; i < phone_query_count; i++) {
        char target[32];

        build_jungle_phone(((i * 7919) % record_count) + 1, target, sizeof(target));
        set_eq_select_value(&stmt, target);
        if (!execute_select_internal(&stmt, 0, 0, &matched_rows)) return;
        if (matched_rows <= 0) {
            printf("[error] jungle benchmark phone lookup returned no rows for '%s'.\n", target);
            return;
        }
        matched_checks++;
    }
    end = current_seconds();
    phone_time = end - start;

    init_eq_select_statement(&stmt, JUNGLE_BENCHMARK_TABLE, "name");
    start = current_seconds();
    for (i = 0; i < linear_query_count; i++) {
        char target[64];

        build_jungle_name(((i * 7919) % record_count) + 1, target, sizeof(target));
        set_eq_select_value(&stmt, target);
        if (!execute_select_internal(&stmt, 0, 0, &matched_rows)) return;
        if (matched_rows <= 0) {
            printf("[error] jungle benchmark name lookup returned no rows for '%s'.\n", target);
            return;
        }
        matched_checks++;
    }
    end = current_seconds();
    linear_time = end - start;

    printf("records: %d\n", record_count);
    printf("id SELECT via SQL path using B+ tree: %.6f sec total (%d queries, %.9f sec/query)\n",
           id_time, id_query_count, id_time / id_query_count);
    printf("email(UK) SELECT via SQL path using B+ tree: %.6f sec total (%d queries, %.9f sec/query)\n",
           email_time, email_query_count, email_time / email_query_count);
    printf("phone(UK) SELECT via SQL path using B+ tree: %.6f sec total (%d queries, %.9f sec/query)\n",
           phone_time, phone_query_count, phone_time / phone_query_count);
    printf("name SELECT via SQL path using linear scan: %.6f sec total (%d queries, %.9f sec/query)\n",
           linear_time, linear_query_count, linear_time / linear_query_count);
    if (id_time > 0.0) {
        double index_avg = id_time / id_query_count;
        double linear_avg = linear_time / linear_query_count;
        printf("linear/id-index average speed ratio: %.2fx\n", linear_avg / index_avg);
    }
    if (email_time > 0.0) {
        double email_avg = email_time / email_query_count;
        double linear_avg = linear_time / linear_query_count;
        printf("linear/email-index average speed ratio: %.2fx\n", linear_avg / email_avg);
    }
    if (phone_time > 0.0) {
        double phone_avg = phone_time / phone_query_count;
        double linear_avg = linear_time / linear_query_count;
        printf("linear/phone-index average speed ratio: %.2fx\n", linear_avg / phone_avg);
    }
    printf("matched checks: %d\n", matched_checks);
    fflush(stdout);
}

void run_bplus_benchmark(int record_count) {
    FILE *f;
    TableCache *tc;
    const char *table_name = "bptree_perf_users";
    int i;
    int index_query_count = 50000;
    int uk_query_count = 50000;
    int linear_query_count = 1;
    int found = 0;
    double start;
    double end;
    double id_indexed_time;
    double uk_indexed_time;
    double linear_time;
    double update_time;
    double delete_time;
    BPlusPair *id_pairs = NULL;
    BPlusStringPair *uk_pairs = NULL;
    char (*name_values)[32] = NULL;
    long *id_targets = NULL;
    char (*uk_targets)[64] = NULL;

    if (record_count <= 0) record_count = 1;
    if (record_count > MAX_RECORDS) {
        INFO_PRINTF("[notice] benchmark record count capped at MAX_RECORDS=%d.\n", MAX_RECORDS);
        record_count = MAX_RECORDS;
    }

    close_all_tables();
    open_table_count = 0;
    remove("bptree_perf_users.delta");

    f = fopen("bptree_perf_users.csv", "wb");
    if (!f) {
        printf("[error] benchmark table file could not be created.\n");
        return;
    }
    if (fprintf(f, "id(PK),email(UK),payload(NN),name\n") < 0 || fclose(f) != 0) {
        printf("[error] benchmark table header could not be written.\n");
        return;
    }

    tc = get_table(table_name);
    if (!tc) return;

    id_pairs = (BPlusPair *)calloc((size_t)record_count, sizeof(BPlusPair));
    uk_pairs = (BPlusStringPair *)calloc((size_t)record_count, sizeof(BPlusStringPair));
    name_values = (char (*)[32])calloc((size_t)record_count, sizeof(*name_values));
    id_targets = (long *)calloc((size_t)index_query_count, sizeof(*id_targets));
    uk_targets = (char (*)[64])calloc((size_t)uk_query_count, sizeof(*uk_targets));
    if (!id_pairs || !uk_pairs || !name_values || !id_targets || !uk_targets) {
        printf("[error] benchmark pair arrays could not be allocated.\n");
        free(id_pairs);
        free(uk_pairs);
        free(name_values);
        free(id_targets);
        free(uk_targets);
        return;
    }

    for (i = 0; i < index_query_count; i++) {
        id_targets[i] = (long)((i * 7919) % record_count) + 1;
    }
    for (i = 0; i < uk_query_count; i++) {
        snprintf(uk_targets[i], sizeof(uk_targets[i]), "user%07d@test.com", ((i * 7919) % record_count) + 1);
    }

    start = current_seconds();
    for (i = 1; i <= record_count; i++) {
        char new_line[RECORD_SIZE];
        char email[64];
        long row_offset;
        long row_id;
        int inserted_slot = -1;

        snprintf(email, sizeof(email), "user%07d@test.com", i);
        snprintf(new_line, sizeof(new_line), "%d,%s,payload%d,User%d", i, email, i, i);
        snprintf(name_values[i - 1], sizeof(name_values[i - 1]), "User%d", i);
        if (tc->append_offset < 0) {
            if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_END) != 0) {
                printf("[error] benchmark append offset failed.\n");
                free(id_pairs);
                free(uk_pairs);
                free(name_values);
                free(id_targets);
                free(uk_targets);
                return;
            }
            tc->append_offset = ftell(tc->file);
        }
        row_offset = tc->append_offset;
        if (!append_record_file(tc, new_line, 0)) {
            printf("[error] benchmark append failed at row %d.\n", i);
            free(id_pairs);
            free(uk_pairs);
            free(name_values);
            free(id_targets);
            free(uk_targets);
            return;
        }
        row_id = tc->next_row_id++;
        if (!append_record_raw_memory(tc, new_line, row_id, row_offset, &inserted_slot)) {
            printf("[error] benchmark insert failed at row %d.\n", i);
            free(id_pairs);
            free(uk_pairs);
            free(name_values);
            free(id_targets);
            free(uk_targets);
            return;
        }
        id_pairs[i - 1].key = i;
        id_pairs[i - 1].row_index = inserted_slot;
        uk_pairs[i - 1].key = dup_string(email);
        if (!uk_pairs[i - 1].key) {
            printf("[error] benchmark UK key allocation failed at row %d.\n", i);
            free(id_pairs);
            free(uk_pairs);
            free(name_values);
            free(id_targets);
            free(uk_targets);
            return;
        }
        uk_pairs[i - 1].row_index = inserted_slot;
    }
    if (fflush(tc->file) != 0 || ferror(tc->file)) {
        printf("[error] benchmark insert flush failed.\n");
        free(id_pairs);
        free(uk_pairs);
        free(name_values);
        free(id_targets);
        free(uk_targets);
        return;
    }
    tc->next_auto_id = (long)record_count + 1;
    if (!bptree_build_from_sorted(tc->id_index, id_pairs, record_count)) {
        printf("[error] benchmark PK B+ tree bulk build failed.\n");
        free(id_pairs);
        free(uk_pairs);
        free(name_values);
        free(id_targets);
        free(uk_targets);
        return;
    }
    if (!ensure_uk_indexes(tc) || tc->uk_count != 1) {
        printf("[error] benchmark UK index is not available.\n");
        free(id_pairs);
        free(uk_pairs);
        free(name_values);
        free(id_targets);
        free(uk_targets);
        return;
    }
    if (!bptree_string_build_from_sorted(tc->uk_indexes[0]->tree, uk_pairs, record_count)) {
        printf("[error] benchmark UK B+ tree bulk build failed.\n");
        free(id_pairs);
        for (i = 0; i < record_count; i++) free(uk_pairs[i].key);
        free(uk_pairs);
        free(name_values);
        free(id_targets);
        free(uk_targets);
        return;
    }
    end = current_seconds();
    free(id_pairs);
    free(uk_pairs);

    printf("\n--- [B+ TREE BENCHMARK] ---\n");
    printf("bulk-loaded records through RowRef path: %d (%.6f sec)\n", record_count, end - start);
    printf("auto B+ tree order: PK=%d, UK=%d\n",
           bptree_order(tc->id_index),
           bptree_string_order(tc->uk_indexes[0]->tree));
    fflush(stdout);

    start = current_seconds();
    for (i = 0; i < index_query_count; i++) {
        int row_index;
        if (bptree_search(tc->id_index, id_targets[i], &row_index)) found += row_index >= 0;
    }
    end = current_seconds();
    id_indexed_time = end - start;

    start = current_seconds();
    for (i = 0; i < uk_query_count; i++) {
        int row_index;
        if (find_uk_row(tc, 1, uk_targets[i], &row_index)) found += row_index >= 0;
    }
    end = current_seconds();
    uk_indexed_time = end - start;

    start = current_seconds();
    for (i = 0; i < linear_query_count; i++) {
        char target[64];
        int row_index;
        snprintf(target, sizeof(target), "User%d", record_count - ((i * 7919) % record_count));
        for (row_index = 0; row_index < record_count; row_index++) {
            if (strcmp(name_values[row_index], target) == 0) {
                found++;
                break;
            }
        }
    }
    end = current_seconds();
    linear_time = end - start;

    printf("records: %d\n", record_count);
    printf("id SELECT using B+ tree: %.6f sec total (%d queries, %.9f sec/query)\n",
           id_indexed_time, index_query_count, id_indexed_time / index_query_count);
    printf("email(UK) SELECT using B+ tree: %.6f sec total (%d queries, %.9f sec/query)\n",
           uk_indexed_time, uk_query_count, uk_indexed_time / uk_query_count);
    printf("name SELECT using linear scan: %.6f sec total (%d queries, %.9f sec/query)\n",
           linear_time, linear_query_count, linear_time / linear_query_count);
    if (id_indexed_time > 0.0) {
        double index_avg = id_indexed_time / index_query_count;
        double linear_avg = linear_time / linear_query_count;
        printf("linear/id-index average speed ratio: %.2fx\n", linear_avg / index_avg);
    }
    if (uk_indexed_time > 0.0) {
        double uk_avg = uk_indexed_time / uk_query_count;
        double linear_avg = linear_time / linear_query_count;
        printf("linear/uk-index average speed ratio: %.2fx\n", linear_avg / uk_avg);
    }
    fflush(stdout);

    start = current_seconds();
    for (i = 1; i <= record_count; i++) {
        int row_index;
        char *new_copy;
        char new_line[RECORD_SIZE];
        char email[64];

        if (!bptree_search(tc->id_index, i, &row_index)) {
            printf("[error] benchmark UPDATE lookup failed at id %d.\n", i);
            return;
        }
        snprintf(email, sizeof(email), "user%07d@test.com", i);
        snprintf(new_line, sizeof(new_line), "%d,%s,payload%d,Updated%d", i, email, i, i);
        new_copy = dup_string(new_line);
        if (!new_copy) {
            printf("[error] benchmark UPDATE allocation failed at id %d.\n", i);
            return;
        }
        tc->records[row_index] = new_copy;
        tc->row_store[row_index] = ROW_STORE_MEMORY;
        tc->row_offsets[row_index] = -1;
        if (tc->row_refs) {
            tc->row_refs[row_index].store = ROW_STORE_MEMORY;
            tc->row_refs[row_index].offset = -1;
        }
        tc->row_cached[row_index] = 0;
        tc->row_cache_seq[row_index] = 0;
        if (!append_delta_update_key(tc, i, new_copy)) {
            free(new_copy);
            tc->records[row_index] = NULL;
            printf("[error] benchmark UPDATE delta append failed at id %d.\n", i);
            return;
        }
    }
    if (!maybe_compact_delta_log(tc)) {
        printf("[error] benchmark UPDATE delta compaction failed.\n");
        return;
    }
    end = current_seconds();
    update_time = end - start;
    printf("UPDATE by PK using B+ tree: %.6f sec total (%d rows, %.9f sec/row)\n",
           update_time, record_count, update_time / record_count);
    fflush(stdout);

    start = current_seconds();
    for (i = 1; i <= record_count; i++) {
        int row_index = i - 1;

        if (!slot_is_active(tc, row_index)) {
            printf("[error] benchmark DELETE active slot check failed at id %d.\n", i);
            return;
        }
        tc->record_active[row_index] = 0;
        tc->active_count--;
        if (!append_delta_delete_key(tc, i)) {
            tc->record_active[row_index] = 1;
            tc->active_count++;
            printf("[error] benchmark DELETE delta append failed at id %d.\n", i);
            return;
        }
        free(tc->records[row_index]);
        tc->records[row_index] = NULL;
        if (tc->row_cached[row_index] && tc->cached_record_count > 0) tc->cached_record_count--;
        tc->row_cached[row_index] = 0;
        tc->row_cache_seq[row_index] = 0;
        tc->row_store[row_index] = ROW_STORE_NONE;
        tc->row_offsets[row_index] = 0;
        if (tc->row_refs) {
            tc->row_refs[row_index].store = ROW_STORE_NONE;
            tc->row_refs[row_index].offset = 0;
        }
        push_free_slot(tc, row_index);
    }
    if (!maybe_compact_delta_log(tc)) {
        printf("[error] benchmark DELETE delta compaction failed.\n");
        return;
    }
    end = current_seconds();
    delete_time = end - start;

    printf("DELETE by PK using B+ tree: %.6f sec total (%d rows, %.9f sec/row)\n",
           delete_time, record_count, delete_time / record_count);
    printf("post-mutation active rows: %d\n", tc->active_count);
    printf("matched checks: %d\n", found);
    free(name_values);
    free(id_targets);
    free(uk_targets);
}

void close_all_tables(void) {
    int i;
    for (i = 0; i < open_table_count; i++) {
        TableCache *tc = &open_tables[i];

        if (tc->file && !tc->cache_truncated && tc->active_count <= ROW_CACHE_LIMIT && delta_log_exists(tc)) {
            if (!compact_table_file_for_shutdown(tc)) {
                INFO_PRINTF("[warning] failed to compact delta-backed table '%s' during shutdown.\n",
                            tc->table_name);
            }
        }
        free_table_storage(tc);
    }
    open_table_count = 0;
}
