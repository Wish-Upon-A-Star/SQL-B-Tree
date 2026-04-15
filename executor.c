#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#if defined(_WIN32)
#include <windows.h>
#endif

#include "executor.h"
#include "bptree.h"

TableCache open_tables[MAX_TABLES];
int open_table_count = 0;
static unsigned long long g_table_access_seq = 0;

void parse_csv_row(const char *row, char *fields[MAX_COLS], char *buffer);
static void normalize_value(const char *src, char *dest, size_t dest_size);
static int rebuild_id_index(TableCache *tc);
static int rebuild_uk_indexes(TableCache *tc);
static int index_record_uks(TableCache *tc, int row_index);
static int get_uk_slot(TableCache *tc, int col_idx);
static int ensure_uk_indexes(TableCache *tc);
static int rollback_last_record_memory(TableCache *tc);
static void rollback_updated_records(TableCache *tc, char **old_records);
static int parse_long_value(const char *value, long *out);
static int table_file_has_value(TableCache *tc, int col_idx, const char *value);
static int for_each_file_row_from(TableCache *tc, long start_offset,
                                  int (*visitor)(TableCache *, const char *, void *),
                                  void *ctx);

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
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    char existing_key[RECORD_SIZE];

    if (!index || !key || strlen(key) == 0) return 0;
    if (!bptree_string_search(index->tree, key, &found_row)) return 0;
    if (found_row < 0 || found_row >= tc->record_count) return 0;

    parse_csv_row(tc->records[found_row], fields, row_buf);
    normalize_value(fields[index->col_idx], existing_key, sizeof(existing_key));
    if (strcmp(existing_key, key) == 0) {
        if (row_index) *row_index = found_row;
        return 1;
    }
    return 0;
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

    if (!tc || tc->pk_idx == -1 || !value) return 0;
    if (!parse_long_value(value, &key)) return 0;
    return bptree_search(tc->id_index, key, row_index);
}

static int unique_index_insert(TableCache *tc, UniqueIndex *index, const char *key, int row_index) {
    int result;

    if (!index || !key || strlen(key) == 0) return 1;
    if (unique_index_find(tc, index, key, NULL)) return 0;
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
    tc->records = new_records;
    tc->record_capacity = new_capacity;
    return 1;
}

static int append_record_raw_memory(TableCache *tc, const char *row) {
    int row_index;

    if (tc->record_count >= MAX_RECORDS) return 0;
    if (!ensure_record_capacity(tc, tc->record_count + 1)) return 0;

    row_index = tc->record_count;
    tc->records[row_index] = dup_string(row);
    if (!tc->records[row_index]) return 0;
    tc->record_count++;
    return 1;
}

static int append_record_memory(TableCache *tc, const char *row, long id_value) {
    int row_index;

    if (tc->record_count >= MAX_RECORDS) return 0;
    if (!ensure_record_capacity(tc, tc->record_count + 1)) return 0;

    row_index = tc->record_count;
    tc->records[row_index] = dup_string(row);
    if (!tc->records[row_index]) return 0;
    tc->record_count++;

    if (tc->pk_idx != -1) {
        if (bptree_insert(tc->id_index, id_value, row_index) != 1) {
            free(tc->records[row_index]);
            tc->record_count--;
            return 0;
        }
        if (id_value >= tc->next_auto_id) tc->next_auto_id = id_value + 1;
    }
    if (!index_record_uks(tc, row_index)) {
        free(tc->records[row_index]);
        tc->record_count--;
        if (!rebuild_id_index(tc) || !rebuild_uk_indexes(tc)) {
            printf("[error] INSERT rollback failed: indexes may be stale.\n");
        }
        return 0;
    }
    return 1;
}

static int rollback_last_record_memory(TableCache *tc) {
    if (!tc || tc->record_count <= 0) return 0;
    tc->record_count--;
    free(tc->records[tc->record_count]);
    tc->records[tc->record_count] = NULL;
    return rebuild_id_index(tc) && rebuild_uk_indexes(tc);
}

static void rollback_updated_records(TableCache *tc, char **old_records) {
    int i;

    if (!tc || !old_records) return;
    for (i = 0; i < tc->record_count; i++) {
        if (old_records[i]) {
            free(tc->records[i]);
            tc->records[i] = old_records[i];
            old_records[i] = NULL;
        }
    }
}

static void free_table_storage(TableCache *tc) {
    int i;

    if (!tc) return;
    if (tc->file) {
        fclose(tc->file);
        tc->file = NULL;
    }
    for (i = 0; i < tc->record_count; i++) free(tc->records[i]);
    free(tc->records);
    tc->records = NULL;
    tc->record_capacity = 0;
    tc->record_count = 0;
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
    tc->pk_idx = -1;
    tc->next_auto_id = 1;
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
    if (!tc->file) return 0;
    if (fseek(tc->file, 0, SEEK_END) != 0) return 0;
    if (fprintf(tc->file, "%s\n", row) < 0) return 0;
    if (flush_now && fflush(tc->file) != 0) return 0;
    if (ferror(tc->file)) return 0;
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

int rewrite_file(TableCache *tc) {
    char filename[300];
    char temp_filename[320];
    FILE *out;
    int i;

    if (tc->file) fclose(tc->file);
    tc->file = NULL;
    snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", filename);

    out = fopen(temp_filename, "w");
    if (!out) return 0;

    if (!write_table_header(out, tc)) goto fail;
    for (i = 0; i < tc->record_count; i++) {
        if (fprintf(out, "%s\n", tc->records[i]) < 0) goto fail;
    }
    if (fflush(out) != 0 || ferror(out)) goto fail;
    if (fclose(out) != 0) {
        remove(temp_filename);
        tc->file = fopen(filename, "r+");
        return 0;
    }
    if (!replace_table_file(temp_filename, filename)) {
        remove(temp_filename);
        tc->file = fopen(filename, "r+");
        return 0;
    }
    tc->file = fopen(filename, "r+");
    if (!tc->file) {
        printf("[warning] table file was rewritten, but could not be reopened for append.\n");
    }
    return 1;

fail:
    fclose(out);
    remove(temp_filename);
    tc->file = fopen(filename, "r+");
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

    if (tc->pk_idx == -1) {
        bptree_destroy(tc->id_index);
        tc->id_index = new_index;
        tc->next_auto_id = next_auto_id;
        return 1;
    }
    if (tc->record_count > 0) {
        pairs = (BPlusPair *)calloc((size_t)tc->record_count, sizeof(BPlusPair));
        if (!pairs) {
            bptree_destroy(new_index);
            return 0;
        }
    }
    for (i = 0; i < tc->record_count; i++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        long id_value;

        parse_csv_row(tc->records[i], fields, row_buf);
        if (fields[tc->pk_idx] && parse_long_value(fields[tc->pk_idx], &id_value)) {
            if (pair_count > 0 && pairs[pair_count - 1].key >= id_value) sorted = 0;
            pairs[pair_count].key = id_value;
            pairs[pair_count].row_index = i;
            pair_count++;
            if (id_value >= next_auto_id) next_auto_id = id_value + 1;
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
    tc->next_auto_id = next_auto_id;
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
    int i;

    if (tc->uk_count == 0) return 1;
    if (!ensure_uk_indexes(tc)) return 0;
    parse_csv_row(tc->records[row_index], fields, row_buf);
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
        if (tc->record_count > 0) {
            pairs[i] = (BPlusStringPair *)calloc((size_t)tc->record_count, sizeof(BPlusStringPair));
            if (!pairs[i]) goto fail;
        }
    }

    for (row_index = 0; row_index < tc->record_count; row_index++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};

        parse_csv_row(tc->records[row_index], fields, row_buf);
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

static int load_table_contents(TableCache *tc, const char *name, FILE *f) {
    char header[RECORD_SIZE];
    char line[RECORD_SIZE];
    long line_number = 1;
    long file_next_auto_id = 1;

    reset_table_cache(tc);
    if (!tc->id_index) return 0;
    strncpy(tc->table_name, name, sizeof(tc->table_name) - 1);
    tc->file = f;
    touch_table(tc);

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

    while (1) {
        char *nl;
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        long id_value = 0;
        long row_offset = ftell(f);
        size_t line_len;

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

        if (tc->record_count < MAX_RECORDS) {
            if (!append_record_raw_memory(tc, line)) {
                printf("[error] failed to load row into memory.\n");
                return 0;
            }
        } else {
            if (!tc->cache_truncated) tc->uncached_start_offset = row_offset;
            tc->cache_truncated = 1;
        }
    }
    if (!rebuild_id_index(tc) || !rebuild_uk_indexes(tc)) {
        printf("[error] failed to bulk-build indexes while loading '%s'.\n", name);
        return 0;
    }
    if (file_next_auto_id > tc->next_auto_id) tc->next_auto_id = file_next_auto_id;
    if (tc->cache_truncated) {
        printf("[notice] table '%s' exceeded memory cache limit (%d rows). Extra rows stay on disk and use slower CSV scans.\n",
               name, MAX_RECORDS);
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
    f = fopen(filename, "r+");
    if (!f) {
        printf("[notice] '%s.csv' does not exist.\n", name);
        return NULL;
    }

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
    f = fopen(filename, "r+");
    if (!f) return 0;
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

        if (bptree_search(tc->id_index, *id_value, NULL)) {
            printf("[error] INSERT failed: duplicate PK value %ld.\n", *id_value);
            return 0;
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

    if (!tc || !new_line || (!tc->cache_truncated && tc->record_count < MAX_RECORDS)) return 1;
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

    if (!tc) return 0;
    parse_csv_row(row_data, vals, buffer);
    while (val_count < MAX_COLS && vals[val_count]) val_count++;

    if (!build_insert_row(tc, vals, val_count, &id_value, new_line, sizeof(new_line))) return 0;

    if (!validate_file_duplicates_for_uncached_insert(tc, new_line)) return 0;

    if (!tc->cache_truncated && tc->record_count < MAX_RECORDS) {
        if (!append_record_memory(tc, new_line, id_value)) {
            printf("[error] INSERT failed: could not update B+ tree index or memory store.\n");
            return 0;
        }
        if (!append_record_file(tc, new_line, flush_now)) {
            if (!rollback_last_record_memory(tc)) {
                printf("[error] INSERT rollback failed: indexes may be stale.\n");
            }
            printf("[error] INSERT failed: could not append to table file.\n");
            return 0;
        }
    } else {
        long append_offset;

        if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_END) != 0) {
            printf("[error] INSERT failed: could not find append position.\n");
            return 0;
        }
        append_offset = ftell(tc->file);
        if (append_offset < 0) {
            printf("[error] INSERT failed: could not find append position.\n");
            return 0;
        }
        if (!append_record_file(tc, new_line, flush_now)) {
            printf("[error] INSERT failed: could not append to table file.\n");
            return 0;
        }
        if (!tc->cache_truncated) tc->uncached_start_offset = append_offset;
        tc->cache_truncated = 1;
        if (id_value >= tc->next_auto_id) tc->next_auto_id = id_value + 1;
        printf("[notice] INSERT appended to CSV only; memory cache limit is %d rows, so later lookup may use slower file scan.\n",
               MAX_RECORDS);
    }
    if (inserted_id) *inserted_id = id_value;
    return 1;
}

void execute_insert(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    long id_value = 0;

    if (!tc) return;
    if (!insert_row_data(tc, stmt->row_data, 1, &id_value)) return;
    printf("[ok] INSERT completed. id=%ld\n", id_value);
}

static void print_selected_row(const char *row, int select_idx[MAX_COLS], int select_count, int select_all) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    int j;

    if (select_all) {
        printf("%s\n", row);
        return;
    }
    parse_csv_row(row, fields, row_buf);
    for (j = 0; j < select_count; j++) {
        if (j > 0) printf(",");
        printf("%s", fields[select_idx[j]] ? fields[select_idx[j]] : "");
    }
    printf("\n");
}

static void execute_select_file_scan(TableCache *tc, long start_offset, int where_idx,
                                     int select_idx[MAX_COLS], int select_count,
                                     int select_all, const char *where_val) {
    char line[RECORD_SIZE];

    if (!tc || !tc->file) return;
    if (fflush(tc->file) != 0) {
        printf("[error] SELECT failed: could not scan table file.\n");
        return;
    }
    if (start_offset > 0) {
        if (fseek(tc->file, start_offset, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not scan uncached table rows.\n");
            return;
        }
        printf("[scan] uncached CSV tail scan from offset %ld\n", start_offset);
    } else {
        if (fseek(tc->file, 0, SEEK_SET) != 0) {
            printf("[error] SELECT failed: could not scan table file.\n");
            return;
        }
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return;
        }
        printf("[scan] full CSV scan\n");
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
        if (where_idx != -1) {
            char row_buf[RECORD_SIZE];
            char *fields[MAX_COLS] = {0};
            parse_csv_row(line, fields, row_buf);
            if (!compare_value(fields[where_idx], where_val)) continue;
        }
        print_selected_row(line, select_idx, select_count, select_all);
    }
    fseek(tc->file, 0, SEEK_END);
}

void execute_select(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    int where_idx;
    int select_idx[MAX_COLS];
    int select_count = 0;
    int i;

    if (!tc) return;
    where_idx = get_col_idx(tc, stmt->where_col);

    if (!stmt->select_all) {
        for (i = 0; i < stmt->select_col_count; i++) {
            int idx = get_col_idx(tc, stmt->select_cols[i]);
            if (idx == -1) {
                printf("[error] SELECT failed: unknown column '%s'.\n", stmt->select_cols[i]);
                return;
            }
            select_idx[i] = idx;
        }
        select_count = stmt->select_col_count;
    }

    printf("\n--- [SELECT RESULT] table=%s ---\n", tc->table_name);
    if (where_idx == tc->pk_idx && where_idx != -1) {
        long key;
        int row_index;
        if (!parse_long_value(stmt->where_val, &key)) {
            printf("[error] SELECT failed: id condition must be an integer.\n");
            return;
        }
        printf("[index] B+ tree id lookup\n");
        if (bptree_search(tc->id_index, key, &row_index)) {
            print_selected_row(tc->records[row_index], select_idx, select_count, stmt->select_all);
            return;
        }
        if (tc->cache_truncated) {
            execute_select_file_scan(tc, tc->uncached_start_offset, where_idx, select_idx, select_count,
                                     stmt->select_all, stmt->where_val);
        }
        return;
    }

    if (where_idx != -1 && tc->cols[where_idx].type == COL_UK) {
        int row_index;
        printf("[index] UK B+ tree lookup on column '%s'\n", stmt->where_col);
        if (find_uk_row(tc, where_idx, stmt->where_val, &row_index)) {
            print_selected_row(tc->records[row_index], select_idx, select_count, stmt->select_all);
            return;
        }
        if (tc->cache_truncated) {
            execute_select_file_scan(tc, tc->uncached_start_offset, where_idx, select_idx, select_count,
                                     stmt->select_all, stmt->where_val);
        }
        return;
    }

    if (where_idx != -1) printf("[scan] linear scan on column '%s'\n", stmt->where_col);
    for (i = 0; i < tc->record_count; i++) {
        if (where_idx != -1) {
            char row_buf[RECORD_SIZE];
            char *fields[MAX_COLS] = {0};
            parse_csv_row(tc->records[i], fields, row_buf);
            if (!compare_value(fields[where_idx], stmt->where_val)) continue;
        }
        print_selected_row(tc->records[i], select_idx, select_count, stmt->select_all);
    }
    if (tc->cache_truncated) {
        execute_select_file_scan(tc, tc->uncached_start_offset, where_idx, select_idx, select_count,
                                 stmt->select_all, stmt->where_val);
    }
}

static int rewrite_truncated_update(TableCache *tc, int where_idx, const char *where_val,
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
        matched = compare_value(fields[where_idx], where_val);
        if (matched) target_count++;
        if (!matched && tc->cols[set_idx].type == COL_UK && strlen(set_value) > 0 &&
            compare_value(fields[set_idx], set_value)) {
            uk_conflict = 1;
        }
    }
    if (target_count == 0) {
        printf("[notice] no rows matched UPDATE condition.\n");
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
    out = fopen(temp_filename, "w");
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
        matched = compare_value(fields[where_idx], where_val);
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
        tc->file = fopen(filename, "r+");
        return 0;
    }
    if (!reload_table_cache(tc)) return 0;
    printf("[ok] UPDATE completed with CSV scan fallback. rows=%d\n", count);
    return 1;

fail:
    fclose(out);
    remove(temp_filename);
    fseek(tc->file, 0, SEEK_END);
    return 0;
}

static int rewrite_truncated_delete(TableCache *tc, int where_idx, const char *where_val) {
    char filename[300];
    char temp_filename[320];
    char line[RECORD_SIZE];
    FILE *out;
    int count = 0;

    if (!tc || !tc->file) return 0;
    snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    snprintf(temp_filename, sizeof(temp_filename), "%s.tmp", filename);
    out = fopen(temp_filename, "w");
    if (!out) return 0;
    if (!write_table_header(out, tc)) goto fail;

    if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_SET) != 0) goto fail;
    if (!fgets(line, sizeof(line), tc->file)) goto fail;
    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        int matched;

        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        parse_csv_row(line, fields, row_buf);
        matched = compare_value(fields[where_idx], where_val);
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
        printf("[notice] no rows matched DELETE condition.\n");
        fseek(tc->file, 0, SEEK_END);
        return 1;
    }
    fclose(tc->file);
    tc->file = NULL;
    if (!replace_table_file(temp_filename, filename)) {
        remove(temp_filename);
        tc->file = fopen(filename, "r+");
        return 0;
    }
    if (!reload_table_cache(tc)) return 0;
    printf("[ok] DELETE completed with CSV scan fallback. rows=%d\n", count);
    return 1;

fail:
    fclose(out);
    remove(temp_filename);
    fseek(tc->file, 0, SEEK_END);
    return 0;
}

void execute_update(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    int where_idx;
    int set_idx;
    char set_value[256];
    int *match_flags;
    char **old_records;
    int target_count = 0;
    int uses_pk_lookup = 0;
    int target_row = -1;
    int rebuild_uk_needed;
    int i;

    if (!tc) return;
    where_idx = get_col_idx(tc, stmt->where_col);
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
        if (!rewrite_truncated_update(tc, where_idx, stmt->where_val, set_idx, set_value)) {
            printf("[error] UPDATE failed while using CSV scan fallback.\n");
        }
        return;
    }
    rebuild_uk_needed = (tc->cols[set_idx].type == COL_UK);
    uses_pk_lookup = (where_idx == tc->pk_idx);

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

    if (uses_pk_lookup) {
        printf("[index] B+ tree id lookup for UPDATE\n");
        if (find_pk_row(tc, stmt->where_val, &target_row)) {
            match_flags[target_row] = 1;
            target_count = 1;
        }
    } else {
        for (i = 0; i < tc->record_count; i++) {
            char row_buf[RECORD_SIZE];
            char *f[MAX_COLS] = {0};
            parse_csv_row(tc->records[i], f, row_buf);
            if (compare_value(f[where_idx], stmt->where_val)) {
                match_flags[i] = 1;
                target_count++;
            }
        }
    }
    if (target_count == 0) {
        free(old_records);
        free(match_flags);
        printf("[notice] no rows matched UPDATE condition.\n");
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

        if (!match_flags[i]) continue;
        parse_csv_row(tc->records[i], fields, row_buf);
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
        old_records[i] = tc->records[i];
        tc->records[i] = new_copy;
        count++;
    }

    free(match_flags);
    if (count > 0) {
        if (rebuild_uk_needed && !rebuild_uk_indexes(tc)) {
            rollback_updated_records(tc, old_records);
            free(old_records);
            printf("[error] UPDATE failed: UK index rebuild failed; memory restored.\n");
            return;
        }
        if (!rewrite_file(tc)) {
            rollback_updated_records(tc, old_records);
            if (rebuild_uk_needed) rebuild_uk_indexes(tc);
            free(old_records);
            printf("[error] UPDATE warning: memory changed but file rewrite failed.\n");
            return;
        }
        for (i = 0; i < tc->record_count; i++) free(old_records[i]);
        free(old_records);
        printf("[ok] UPDATE completed. rows=%d\n", count);
    } else {
        free(old_records);
        printf("[notice] no rows matched UPDATE condition.\n");
    }
}

void execute_delete(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    int where_idx;
    int count = 0;
    int read_idx;
    int write_idx = 0;
    int old_count;
    char **old_records;
    int *delete_flags;
    int uses_pk_lookup = 0;
    int target_row = -1;

    if (!tc) return;
    where_idx = get_col_idx(tc, stmt->where_col);
    if (where_idx == -1) {
        printf("[error] DELETE failed: WHERE column does not exist.\n");
        return;
    }
    if (tc->cache_truncated) {
        if (!rewrite_truncated_delete(tc, where_idx, stmt->where_val)) {
            printf("[error] DELETE failed while using CSV scan fallback.\n");
        }
        return;
    }

    old_count = tc->record_count;
    uses_pk_lookup = (where_idx == tc->pk_idx);
    if (old_count == 0) {
        printf("[notice] no rows matched DELETE condition.\n");
        return;
    }
    old_records = (char **)malloc((size_t)old_count * sizeof(char *));
    delete_flags = (int *)calloc((size_t)old_count, sizeof(int));
    if (!old_records || !delete_flags) {
        free(old_records);
        free(delete_flags);
        printf("[error] DELETE failed: out of memory.\n");
        return;
    }
    memcpy(old_records, tc->records, (size_t)old_count * sizeof(char *));

    if (uses_pk_lookup) {
        printf("[index] B+ tree id lookup for DELETE\n");
        if (find_pk_row(tc, stmt->where_val, &target_row)) {
            delete_flags[target_row] = 1;
            count = 1;
        }
    } else {
        for (read_idx = 0; read_idx < tc->record_count; read_idx++) {
            char row_buf[RECORD_SIZE];
            char *fields[MAX_COLS] = {0};
            int matched;

            parse_csv_row(tc->records[read_idx], fields, row_buf);
            matched = compare_value(fields[where_idx], stmt->where_val);
            if (matched) {
                delete_flags[read_idx] = 1;
                count++;
            }
        }
    }

    for (read_idx = 0; read_idx < tc->record_count; read_idx++) {
        if (!delete_flags[read_idx]) tc->records[write_idx++] = tc->records[read_idx];
    }
    tc->record_count = write_idx;

    if (count > 0) {
        if (!rebuild_id_index(tc) || !rebuild_uk_indexes(tc)) {
            memcpy(tc->records, old_records, (size_t)old_count * sizeof(char *));
            tc->record_count = old_count;
            rebuild_id_index(tc);
            rebuild_uk_indexes(tc);
            free(old_records);
            free(delete_flags);
            printf("[error] DELETE failed: index rebuild failed; memory restored.\n");
            return;
        }
        if (!rewrite_file(tc)) {
            memcpy(tc->records, old_records, (size_t)old_count * sizeof(char *));
            tc->record_count = old_count;
            rebuild_id_index(tc);
            rebuild_uk_indexes(tc);
            free(old_records);
            free(delete_flags);
            printf("[error] DELETE warning: memory changed but file rewrite failed.\n");
            return;
        }
        for (read_idx = 0; read_idx < old_count; read_idx++) {
            if (delete_flags[read_idx]) free(old_records[read_idx]);
        }
        free(old_records);
        free(delete_flags);
        printf("[ok] DELETE completed. rows=%d\n", count);
    } else {
        free(old_records);
        free(delete_flags);
        printf("[notice] no rows matched DELETE condition.\n");
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

void run_bplus_benchmark(int record_count) {
    FILE *f;
    TableCache *tc;
    const char *table_name = "bptree_benchmark_users";
    int i;
    int index_query_count = 100000;
    int uk_query_count = 100000;
    int linear_query_count = 30;
    int found = 0;
    double start;
    double end;
    double id_indexed_time;
    double uk_indexed_time;
    double linear_time;

    if (record_count < 1000000) record_count = 1000000;
    if (record_count > MAX_RECORDS) {
        printf("[notice] benchmark record count capped at MAX_RECORDS=%d.\n", MAX_RECORDS);
        record_count = MAX_RECORDS;
    }

    close_all_tables();
    open_table_count = 0;

    f = fopen("bptree_benchmark_users.csv", "w");
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

    start = current_seconds();
    for (i = 1; i <= record_count; i++) {
        char row_data[128];
        snprintf(row_data, sizeof(row_data), "user%d@test.com,payload%d,User%d", i, i, i);
        if (!insert_row_data(tc, row_data, 0, NULL)) {
            printf("[error] benchmark insert failed at row %d.\n", i);
            return;
        }
    }
    if (fflush(tc->file) != 0 || ferror(tc->file)) {
        printf("[error] benchmark insert flush failed.\n");
        return;
    }
    end = current_seconds();

    printf("\n--- [B+ TREE BENCHMARK] ---\n");
    printf("inserted records through INSERT path: %d (%.6f sec)\n", record_count, end - start);

    start = current_seconds();
    for (i = 0; i < index_query_count; i++) {
        long key = (long)((i * 7919) % record_count) + 1;
        int row_index;
        if (bptree_search(tc->id_index, key, &row_index)) found += row_index >= 0;
    }
    end = current_seconds();
    id_indexed_time = end - start;

    start = current_seconds();
    for (i = 0; i < uk_query_count; i++) {
        char target[64];
        int row_index;
        snprintf(target, sizeof(target), "user%d@test.com", ((i * 7919) % record_count) + 1);
        if (find_uk_row(tc, 1, target, &row_index)) found += row_index >= 0;
    }
    end = current_seconds();
    uk_indexed_time = end - start;

    start = current_seconds();
    for (i = 0; i < linear_query_count; i++) {
        char target[64];
        int r;
        snprintf(target, sizeof(target), "User%d", ((i * 7919) % record_count) + 1);
        for (r = 0; r < tc->record_count; r++) {
            char row_buf[RECORD_SIZE];
            char *fields[MAX_COLS] = {0};
            parse_csv_row(tc->records[r], fields, row_buf);
            if (compare_value(fields[3], target)) {
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
    printf("matched checks: %d\n", found);
}

void close_all_tables(void) {
    int i;
    for (i = 0; i < open_table_count; i++) free_table_storage(&open_tables[i]);
    open_table_count = 0;
}
