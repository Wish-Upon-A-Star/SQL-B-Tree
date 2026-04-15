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
static void rebuild_id_index(TableCache *tc);
static int rebuild_uk_indexes(TableCache *tc);
static int index_record_uks(TableCache *tc, int row_index);

static char *dup_string(const char *src) {
    size_t len = strlen(src) + 1;
    char *copy = (char *)malloc(len);
    if (!copy) return NULL;
    memcpy(copy, src, len);
    return copy;
}

typedef struct UniqueEntry {
    unsigned long long hash;
    int row_index;
    struct UniqueEntry *next;
} UniqueEntry;

struct UniqueIndex {
    UniqueEntry **buckets;
    size_t bucket_count;
    size_t count;
    int col_idx;
};

static unsigned long long hash_string(const char *s) {
    unsigned long long hash = 1469598103934665603ULL;
    while (*s) {
        hash ^= (unsigned char)*s++;
        hash *= 1099511628211ULL;
    }
    return hash;
}

static UniqueIndex *unique_index_create(int col_idx) {
    UniqueIndex *index = (UniqueIndex *)calloc(1, sizeof(UniqueIndex));
    if (!index) return NULL;
    index->col_idx = col_idx;
    index->bucket_count = 2048;
    index->buckets = (UniqueEntry **)calloc(index->bucket_count, sizeof(UniqueEntry *));
    if (!index->buckets) {
        free(index);
        return NULL;
    }
    return index;
}

static void unique_index_clear(UniqueIndex *index) {
    size_t i;
    if (!index) return;
    for (i = 0; i < index->bucket_count; i++) {
        UniqueEntry *entry = index->buckets[i];
        while (entry) {
            UniqueEntry *next = entry->next;
            free(entry);
            entry = next;
        }
        index->buckets[i] = NULL;
    }
    index->count = 0;
}

static void unique_index_destroy(UniqueIndex *index) {
    if (!index) return;
    unique_index_clear(index);
    free(index->buckets);
    free(index);
}

static int unique_index_rehash(UniqueIndex *index, size_t new_bucket_count) {
    UniqueEntry **new_buckets;
    size_t i;

    new_buckets = (UniqueEntry **)calloc(new_bucket_count, sizeof(UniqueEntry *));
    if (!new_buckets) return 0;
    for (i = 0; i < index->bucket_count; i++) {
        UniqueEntry *entry = index->buckets[i];
        while (entry) {
            UniqueEntry *next = entry->next;
            size_t bucket = entry->hash % new_bucket_count;
            entry->next = new_buckets[bucket];
            new_buckets[bucket] = entry;
            entry = next;
        }
    }
    free(index->buckets);
    index->buckets = new_buckets;
    index->bucket_count = new_bucket_count;
    return 1;
}

static int unique_index_find(TableCache *tc, UniqueIndex *index, const char *key, int *row_index) {
    UniqueEntry *entry;
    unsigned long long hash;
    size_t bucket;

    if (!index || !key || strlen(key) == 0) return 0;
    hash = hash_string(key);
    bucket = hash % index->bucket_count;
    entry = index->buckets[bucket];
    while (entry) {
        if (entry->hash == hash && entry->row_index >= 0 && entry->row_index < tc->record_count) {
            char row_buf[RECORD_SIZE];
            char *fields[MAX_COLS] = {0};
            char existing_key[RECORD_SIZE];

            parse_csv_row(tc->records[entry->row_index], fields, row_buf);
            normalize_value(fields[index->col_idx], existing_key, sizeof(existing_key));
            if (strcmp(existing_key, key) == 0) {
                if (row_index) *row_index = entry->row_index;
                return 1;
            }
        }
        entry = entry->next;
    }
    return 0;
}

static int unique_index_insert(TableCache *tc, UniqueIndex *index, const char *key, int row_index) {
    UniqueEntry *entry;
    unsigned long long hash;
    size_t bucket;

    if (!index || !key || strlen(key) == 0) return 1;
    if (unique_index_find(tc, index, key, NULL)) return 0;
    if ((index->count + 1) * 4 > index->bucket_count * 3) {
        if (!unique_index_rehash(index, index->bucket_count * 2)) return -1;
    }

    entry = (UniqueEntry *)calloc(1, sizeof(UniqueEntry));
    if (!entry) return -1;
    hash = hash_string(key);
    entry->hash = hash;
    entry->row_index = row_index;
    bucket = hash % index->bucket_count;
    entry->next = index->buckets[bucket];
    index->buckets[bucket] = entry;
    index->count++;
    return 1;
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
        rebuild_id_index(tc);
        rebuild_uk_indexes(tc);
        return 0;
    }
    return 1;
}

static void rollback_last_record_memory(TableCache *tc) {
    if (!tc || tc->record_count <= 0) return;
    tc->record_count--;
    free(tc->records[tc->record_count]);
    tc->records[tc->record_count] = NULL;
    rebuild_id_index(tc);
    rebuild_uk_indexes(tc);
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

static int replace_table_file(const char *temp_filename, const char *filename) {
#if defined(_WIN32)
    return MoveFileExA(temp_filename, filename, MOVEFILE_REPLACE_EXISTING) != 0;
#else
    return rename(temp_filename, filename) == 0;
#endif
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

    for (i = 0; i < tc->col_count; i++) {
        if (fprintf(out, "%s", tc->cols[i].name) < 0) goto fail;
        if (tc->cols[i].type == COL_PK && fprintf(out, "(PK)") < 0) goto fail;
        else if (tc->cols[i].type == COL_UK && fprintf(out, "(UK)") < 0) goto fail;
        else if (tc->cols[i].type == COL_NN && fprintf(out, "(NN)") < 0) goto fail;
        if (fprintf(out, "%s", (i == tc->col_count - 1 ? "\n" : ",")) < 0) goto fail;
    }
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
    return tc->file != NULL;

fail:
    fclose(out);
    remove(temp_filename);
    tc->file = fopen(filename, "r+");
    return 0;
}

static void rebuild_id_index(TableCache *tc) {
    int i;

    if (!tc->id_index) tc->id_index = bptree_create();
    else bptree_clear(tc->id_index);
    tc->next_auto_id = 1;

    if (tc->pk_idx == -1) return;
    for (i = 0; i < tc->record_count; i++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        long id_value;

        parse_csv_row(tc->records[i], fields, row_buf);
        if (fields[tc->pk_idx] && parse_long_value(fields[tc->pk_idx], &id_value)) {
            bptree_insert(tc->id_index, id_value, i);
            if (id_value >= tc->next_auto_id) tc->next_auto_id = id_value + 1;
        }
    }
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
    int i;

    if (!ensure_uk_indexes(tc)) return 0;
    for (i = 0; i < tc->uk_count; i++) unique_index_clear(tc->uk_indexes[i]);
    for (i = 0; i < tc->record_count; i++) {
        if (!index_record_uks(tc, i)) return 0;
    }
    return 1;
}

static int load_table_contents(TableCache *tc, const char *name, FILE *f) {
    char header[RECORD_SIZE];
    char line[RECORD_SIZE];
    long line_number = 1;

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

    while (fgets(line, sizeof(line), f)) {
        char *nl = strpbrk(line, "\r\n");
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        long id_value = 0;
        size_t line_len = strlen(line);

        line_number++;
        if (!nl && line_len == sizeof(line) - 1 && !feof(f)) {
            printf("[error] row too long while loading '%s' at line %ld (max %d bytes).\n",
                   name, line_number, RECORD_SIZE - 1);
            return 0;
        }

        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        if (tc->record_count >= MAX_RECORDS) {
            printf("[error] record limit exceeded while loading '%s' (max %d). Table was not loaded partially.\n",
                   name, MAX_RECORDS);
            return 0;
        }

        parse_csv_row(line, fields, row_buf);
        if (tc->pk_idx != -1 && fields[tc->pk_idx] && !parse_long_value(fields[tc->pk_idx], &id_value)) {
            printf("[error] invalid PK value while loading '%s': %s\n", name, fields[tc->pk_idx]);
            return 0;
        }
        if (!append_record_memory(tc, line, id_value)) {
            printf("[error] failed to load row into memory.\n");
            return 0;
        }
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

static int insert_row_data(TableCache *tc, const char *row_data, int flush_now, long *inserted_id) {
    char buffer[RECORD_SIZE];
    char *vals[MAX_COLS] = {0};
    char new_line[RECORD_SIZE] = "";
    int val_count = 0;
    long id_value = 0;

    if (!tc) return 0;
    parse_csv_row(row_data, vals, buffer);
    while (val_count < MAX_COLS && vals[val_count]) val_count++;

    if (tc->record_count >= MAX_RECORDS) {
        printf("[error] INSERT failed: record limit exceeded (max %d).\n", MAX_RECORDS);
        return 0;
    }
    if (!build_insert_row(tc, vals, val_count, &id_value, new_line, sizeof(new_line))) return 0;
    if (!append_record_memory(tc, new_line, id_value)) {
        printf("[error] INSERT failed: could not update B+ tree index or memory store.\n");
        return 0;
    }
    if (!append_record_file(tc, new_line, flush_now)) {
        rollback_last_record_memory(tc);
        printf("[error] INSERT failed: could not append to table file.\n");
        return 0;
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
}

void execute_update(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    int where_idx;
    int set_idx;
    char set_value[256];
    int *match_flags;
    char **old_records;
    int target_count = 0;
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

    for (i = 0; i < tc->record_count; i++) {
        char row_buf[RECORD_SIZE];
        char *f[MAX_COLS] = {0};
        parse_csv_row(tc->records[i], f, row_buf);
        if (compare_value(f[where_idx], stmt->where_val)) {
            match_flags[i] = 1;
            target_count++;
        }
    }
    if (target_count == 0) {
        free(old_records);
        free(match_flags);
        printf("[notice] no rows matched UPDATE condition.\n");
        return;
    }

    if (tc->cols[set_idx].type == COL_UK) {
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
            printf("[error] UPDATE failed: rebuilt row is too long.\n");
            free(old_records);
            free(match_flags);
            return;
        }
        char *new_copy = dup_string(new_row);
        if (!new_copy) {
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
        if (!rewrite_file(tc)) {
            for (i = 0; i < tc->record_count; i++) {
                if (old_records[i]) {
                    free(tc->records[i]);
                    tc->records[i] = old_records[i];
                    old_records[i] = NULL;
                }
            }
            free(old_records);
            printf("[error] UPDATE warning: memory changed but file rewrite failed.\n");
            return;
        }
        for (i = 0; i < tc->record_count; i++) free(old_records[i]);
        free(old_records);
        if (!rebuild_uk_indexes(tc)) {
            printf("[error] UPDATE warning: UK index rebuild failed.\n");
            return;
        }
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

    if (!tc) return;
    where_idx = get_col_idx(tc, stmt->where_col);
    if (where_idx == -1) {
        printf("[error] DELETE failed: WHERE column does not exist.\n");
        return;
    }

    old_count = tc->record_count;
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

    for (read_idx = 0; read_idx < tc->record_count; read_idx++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        int matched;

        parse_csv_row(tc->records[read_idx], fields, row_buf);
        matched = compare_value(fields[where_idx], stmt->where_val);
        if (matched) {
            delete_flags[read_idx] = 1;
            count++;
            continue;
        }
        tc->records[write_idx++] = tc->records[read_idx];
    }
    tc->record_count = write_idx;

    if (count > 0) {
        rebuild_id_index(tc);
        if (!rewrite_file(tc)) {
            memcpy(tc->records, old_records, (size_t)old_count * sizeof(char *));
            tc->record_count = old_count;
            rebuild_id_index(tc);
            free(old_records);
            free(delete_flags);
            printf("[error] DELETE warning: memory changed but file rewrite failed.\n");
            return;
        }
        for (read_idx = 0; read_idx < old_count; read_idx++) {
            if (delete_flags[read_idx]) free(old_records[read_idx]);
        }
        if (!rebuild_uk_indexes(tc)) {
            free(old_records);
            free(delete_flags);
            printf("[error] DELETE warning: UK index rebuild failed.\n");
            return;
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
    int linear_query_count = 30;
    int found = 0;
    double start;
    double end;
    double indexed_time;
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
    indexed_time = end - start;

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
           indexed_time, index_query_count, indexed_time / index_query_count);
    printf("name SELECT using linear scan: %.6f sec total (%d queries, %.9f sec/query)\n",
           linear_time, linear_query_count, linear_time / linear_query_count);
    if (indexed_time > 0.0) {
        double index_avg = indexed_time / index_query_count;
        double linear_avg = linear_time / linear_query_count;
        printf("linear/index average speed ratio: %.2fx\n", linear_avg / index_avg);
    }
    printf("matched checks: %d\n", found);
}

void close_all_tables(void) {
    int i;
    for (i = 0; i < open_table_count; i++) free_table_storage(&open_tables[i]);
    open_table_count = 0;
}
