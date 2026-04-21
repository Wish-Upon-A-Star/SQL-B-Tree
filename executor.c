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
#include "jungle_benchmark.h"

#define DELTA_LINE_SIZE (RECORD_SIZE + 128)
#define TABLE_FILE_BUFFER_SIZE (1024 * 1024)

typedef struct SnapshotMeta SnapshotMeta;
typedef struct MutationLookupPlan MutationLookupPlan;

TableCache open_tables[MAX_TABLES];
int open_table_count = 0;
static unsigned long long g_table_access_seq = 0;
static int g_executor_quiet = 0;

#define INFO_PRINTF(...) do { if (!g_executor_quiet) printf(__VA_ARGS__); } while (0)

struct MutationLookupPlan {
    int condition_index;
    int where_idx;
    int uses_pk_lookup;
    int uses_uk_lookup;
};

void set_executor_quiet(int quiet) {
    g_executor_quiet = quiet ? 1 : 0;
}

void parse_csv_row(const char *row, char *fields[MAX_COLS], char *buffer);
static void normalize_value(const char *src, char *dest, size_t dest_size);
static int rebuild_id_index(TableCache *tc);
static int rebuild_uk_indexes(TableCache *tc);
static int maybe_rebuild_indexes_for_order(TableCache *tc);
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
static void clear_tail_delta(TableCache *tc, long id_value);
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
static int parse_table_header(TableCache *tc, FILE *f);
static int load_rows_into_cache(TableCache *tc, const char *name, FILE *f,
                                int has_delta_log, long *file_next_auto_id_out);
static int finalize_indexes_and_recovery(TableCache *tc, const char *name, FILE *f,
                                         int has_delta_log, long file_next_auto_id);
static int load_table_contents(TableCache *tc, const char *name, FILE *f);
static int execute_update_single_row(TableCache *tc, Statement *stmt, int where_idx,
                                     const WhereCondition *lookup_cond, int set_idx, const char *set_value, int uses_pk_lookup,
                                     int uses_uk_lookup, int rebuild_uk_needed);
static int row_fields_match_statement(TableCache *tc, Statement *stmt, char *fields[MAX_COLS]);
static int mark_selected_rows(TableCache *tc, Statement *stmt, int *match_flags, int *match_count);
static int prepare_update_set(TableCache *tc, Statement *stmt, int *set_idx,
                              char *set_value, size_t set_value_size, int *rebuild_uk_needed);
static int build_updated_row_copy(TableCache *tc, const char *row, int set_idx, const char *set_value,
                                  char **new_copy, int allow_slice_build);
static int build_updated_row_text(TableCache *tc, const char *row, int set_idx, const char *set_value,
                                  char *new_row, size_t new_row_size);
static void set_slot_memory_row(TableCache *tc, int slot_id, char *row);
static int persist_updated_row(TableCache *tc, int target_row, char *old_record,
                               int set_idx, const char *set_value,
                               int rebuild_uk_needed, int allow_slice_build, int emit_logs);
static int validate_update_uk(TableCache *tc, int set_idx, const char *set_value,
                              int target_row, int *match_flags, int target_count);
static int execute_update_scan(TableCache *tc, Statement *stmt, int set_idx, const char *set_value,
                               int rebuild_uk_needed);
static int open_rewrite_output(TableCache *tc, char *filename, size_t filename_size,
                               char *temp_filename, size_t temp_filename_size, FILE **out);
static int commit_rewrite_output(TableCache *tc, FILE *out,
                                 const char *filename, const char *temp_filename);
static int abort_rewrite_output(TableCache *tc, FILE *out, const char *temp_filename);
static int scan_truncated_update_targets(TableCache *tc, Statement *stmt, int set_idx,
                                         const char *set_value, int *target_count, int *uk_conflict);
static int resolve_tail_pk_row(TableCache *tc, Statement *stmt, long pk_key, long offset,
                               char *base_row, size_t base_row_size, const char **current_row);
static int persist_deleted_slot(TableCache *tc, int target_row, char *old_record, int emit_logs);
static int execute_delete_single_row(TableCache *tc, Statement *stmt, int index_col,
                                     const WhereCondition *lookup_cond, int uses_pk_lookup);
static int execute_delete_scan(TableCache *tc, Statement *stmt);
static int scan_select_file_rows(TableCache *tc, long start_offset,
                                 const char *tail_trace, const char *full_trace,
                                 int (*visitor)(TableCache *, const char *, void *), void *ctx);
static void build_mutation_lookup_plan(TableCache *tc, Statement *stmt, MutationLookupPlan *plan);
static int read_csv_row_at_offset(TableCache *tc, long offset, char *line, size_t line_size);
static int read_snapshot_meta(TableCache *tc, FILE *f, SnapshotMeta *meta,
                              int allow_v1, int require_delta_absent);
static int read_snapshot_id_pairs(TableCache *tc, FILE *f, int active_count, BPlusPair **id_pairs_out);
static int read_snapshot_uk_indexes(TableCache *tc, FILE *f, int active_count,
                                    UniqueIndex *new_indexes[MAX_UKS]);
static void reset_deleted_flags(TableCache *tc, int *delete_flags, int old_count);
static void release_deleted_slots(TableCache *tc, int *delete_flags, int old_count);
static void clear_slot_storage(TableCache *tc, int slot_id);
int rewrite_file(TableCache *tc);
static int execute_insert_internal(Statement *stmt, long *inserted_id);
static int execute_update_internal(Statement *stmt, int *affected_rows);
static int execute_delete_internal(Statement *stmt, int *affected_rows);

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

static void clear_slot_storage(TableCache *tc, int slot_id) {
    if (!tc || slot_id < 0 || slot_id >= tc->record_count) return;

    free(tc->records[slot_id]);
    tc->records[slot_id] = NULL;
    if (tc->row_cached[slot_id] && tc->cached_record_count > 0) {
        tc->cached_record_count--;
    }
    tc->row_cached[slot_id] = 0;
    tc->row_cache_seq[slot_id] = 0;
    tc->row_store[slot_id] = ROW_STORE_NONE;
    tc->row_offsets[slot_id] = 0;
    if (tc->row_refs) {
        tc->row_refs[slot_id].store = ROW_STORE_NONE;
        tc->row_refs[slot_id].offset = 0;
    }
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
    int lo = 0;
    int hi;
    int pos;

    if (!tc) return 1;
    hi = tc->tail_count - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (tc->tail_pk_ids[mid] == id_value) {
            tc->tail_offsets[mid] = offset;
            return 1;
        }
        if (tc->tail_pk_ids[mid] < id_value) lo = mid + 1;
        else hi = mid - 1;
    }
    if (!ensure_tail_index_capacity(tc, tc->tail_count + 1)) return 0;
    pos = lo;
    if (tc->tail_count - pos > 0) {
        memmove(tc->tail_pk_ids + pos + 1, tc->tail_pk_ids + pos,
                (size_t)(tc->tail_count - pos) * sizeof(long));
        memmove(tc->tail_offsets + pos + 1, tc->tail_offsets + pos,
                (size_t)(tc->tail_count - pos) * sizeof(long));
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

static int find_tail_delta_index(TableCache *tc, long id_value) {
    int lo = 0;
    int hi;

    if (!tc || tc->tail_delta_count <= 0) return -1;
    hi = tc->tail_delta_count - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (tc->tail_delta_ids[mid] == id_value) return mid;
        if (tc->tail_delta_ids[mid] < id_value) lo = mid + 1;
        else hi = mid - 1;
    }
    return -1;
}

static int ensure_tail_delta_capacity(TableCache *tc, int required) {
    int new_capacity;
    long *new_ids;
    char **new_rows;
    unsigned char *new_deleted;

    if (!tc || required <= tc->tail_delta_capacity) return 1;
    new_capacity = tc->tail_delta_capacity > 0 ? tc->tail_delta_capacity : INITIAL_RECORD_CAPACITY;
    while (new_capacity < required) new_capacity *= 2;
    new_ids = (long *)realloc(tc->tail_delta_ids, (size_t)new_capacity * sizeof(long));
    if (!new_ids) return 0;
    new_rows = (char **)realloc(tc->tail_delta_rows, (size_t)new_capacity * sizeof(char *));
    if (!new_rows) {
        tc->tail_delta_ids = new_ids;
        return 0;
    }
    new_deleted = (unsigned char *)realloc(tc->tail_delta_deleted, (size_t)new_capacity * sizeof(unsigned char));
    if (!new_deleted) {
        tc->tail_delta_ids = new_ids;
        tc->tail_delta_rows = new_rows;
        return 0;
    }
    if (new_capacity > tc->tail_delta_capacity) {
        memset(new_rows + tc->tail_delta_capacity, 0,
               (size_t)(new_capacity - tc->tail_delta_capacity) * sizeof(char *));
        memset(new_deleted + tc->tail_delta_capacity, 0,
               (size_t)(new_capacity - tc->tail_delta_capacity) * sizeof(unsigned char));
    }
    tc->tail_delta_ids = new_ids;
    tc->tail_delta_rows = new_rows;
    tc->tail_delta_deleted = new_deleted;
    tc->tail_delta_capacity = new_capacity;
    return 1;
}

static int set_tail_delta(TableCache *tc, long id_value, const char *row, int deleted) {
    int idx;
    int pos;
    char *copy = NULL;

    if (!tc) return 0;
    if (!deleted) {
        copy = dup_string(row);
        if (!copy) return 0;
    }
    idx = find_tail_delta_index(tc, id_value);
    if (idx >= 0) {
        free(tc->tail_delta_rows[idx]);
        tc->tail_delta_rows[idx] = copy;
        tc->tail_delta_deleted[idx] = deleted ? 1 : 0;
        return 1;
    }
    if (!ensure_tail_delta_capacity(tc, tc->tail_delta_count + 1)) {
        free(copy);
        return 0;
    }
    pos = tc->tail_delta_count;
    while (pos > 0 && tc->tail_delta_ids[pos - 1] > id_value) {
        tc->tail_delta_ids[pos] = tc->tail_delta_ids[pos - 1];
        tc->tail_delta_rows[pos] = tc->tail_delta_rows[pos - 1];
        tc->tail_delta_deleted[pos] = tc->tail_delta_deleted[pos - 1];
        pos--;
    }
    tc->tail_delta_ids[pos] = id_value;
    tc->tail_delta_rows[pos] = copy;
    tc->tail_delta_deleted[pos] = deleted ? 1 : 0;
    tc->tail_delta_count++;
    return 1;
}

static const char *tail_overlay_row(TableCache *tc, long id_value, int *deleted) {
    int idx = find_tail_delta_index(tc, id_value);

    if (deleted) *deleted = 0;
    if (idx < 0) return NULL;
    if (tc->tail_delta_deleted[idx]) {
        if (deleted) *deleted = 1;
        return NULL;
    }
    return tc->tail_delta_rows[idx];
}

static void clear_tail_delta(TableCache *tc, long id_value) {
    int idx = find_tail_delta_index(tc, id_value);

    if (!tc || idx < 0) return;
    free(tc->tail_delta_rows[idx]);
    if (idx + 1 < tc->tail_delta_count) {
        memmove(tc->tail_delta_ids + idx, tc->tail_delta_ids + idx + 1,
                (size_t)(tc->tail_delta_count - idx - 1) * sizeof(long));
        memmove(tc->tail_delta_rows + idx, tc->tail_delta_rows + idx + 1,
                (size_t)(tc->tail_delta_count - idx - 1) * sizeof(char *));
        memmove(tc->tail_delta_deleted + idx, tc->tail_delta_deleted + idx + 1,
                (size_t)(tc->tail_delta_count - idx - 1) * sizeof(unsigned char));
    }
    tc->tail_delta_count--;
    if (tc->tail_delta_count >= 0 && tc->tail_delta_rows) {
        tc->tail_delta_rows[tc->tail_delta_count] = NULL;
        tc->tail_delta_deleted[tc->tail_delta_count] = 0;
    }
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
    if (!maybe_rebuild_indexes_for_order(tc)) {
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
    if (tc->tail_delta_rows) {
        for (i = 0; i < tc->tail_delta_count; i++) free(tc->tail_delta_rows[i]);
    }
    free(tc->tail_delta_ids);
    free(tc->tail_delta_rows);
    free(tc->tail_delta_deleted);
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
    tc->tail_delta_ids = NULL;
    tc->tail_delta_rows = NULL;
    tc->tail_delta_deleted = NULL;
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
    tc->tail_delta_count = 0;
    tc->tail_delta_capacity = 0;
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
    const char *effective = row;

    parse_csv_row(row, fields, row_buf);
    if (tc && tc->cache_truncated && tc->pk_idx >= 0 && fields[tc->pk_idx]) {
        long id_value;
        if (parse_long_value(fields[tc->pk_idx], &id_value)) {
            int deleted = 0;
            const char *overlay = tail_overlay_row(tc, id_value, &deleted);
            if (deleted) return 1;
            if (overlay) {
                effective = overlay;
                parse_csv_row(effective, fields, row_buf);
            }
        }
    }
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
                if (!replace_record_at(tc, row_index, ops[i].row)) return 0;
            } else {
                long tail_offset;
                if (find_tail_pk_offset(tc, ops[i].id, &tail_offset) &&
                    !set_tail_delta(tc, ops[i].id, ops[i].row, 0)) {
                    return 0;
                }
            }
        } else if (ops[i].type == 'D') {
            if (row_index >= 0) {
                if (!delete_record_at(tc, row_index)) return 0;
            } else {
                long tail_offset;
                if (find_tail_pk_offset(tc, ops[i].id, &tail_offset) &&
                    !set_tail_delta(tc, ops[i].id, NULL, 1)) {
                    return 0;
                }
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
    if (tc->tail_delta_rows) {
        for (i = 0; i < tc->tail_delta_count; i++) free(tc->tail_delta_rows[i]);
    }
    free(tc->tail_delta_ids);
    free(tc->tail_delta_rows);
    free(tc->tail_delta_deleted);
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

static int build_updated_row(TableCache *tc, const char *row, int set_idx,
                             const char *set_value, char *new_row, size_t new_row_size) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    size_t offset = 0;
    int j;

    if (!tc || !row || !new_row || new_row_size == 0) return 0;
    new_row[0] = '\0';
    parse_csv_row(row, fields, row_buf);
    for (j = 0; j < tc->col_count; j++) {
        const char *val = (j == set_idx) ? set_value : (fields[j] ? fields[j] : "");
        if (!append_csv_field(new_row, new_row_size, &offset, val, j == tc->col_count - 1)) return 0;
    }
    return 1;
}

static int build_updated_row_slice(TableCache *tc, const char *row, int set_idx,
                                   const char *set_value, char *new_row, size_t new_row_size) {
    const char *field_start;
    const char *field_end;
    const char *p;
    size_t prefix_len;
    size_t offset = 0;
    int col = 0;
    int in_quote = 0;

    if (!tc || !row || !new_row || set_idx < 0 || set_idx >= tc->col_count) return 0;
    field_start = row;
    field_end = row + strlen(row);
    for (p = row; ; p++) {
        if (*p == '\'') in_quote = !in_quote;
        if ((*p == ',' && !in_quote) || *p == '\0') {
            if (col == set_idx) {
                field_end = p;
                break;
            }
            if (*p == '\0') return 0;
            col++;
            field_start = p + 1;
        }
    }

    prefix_len = (size_t)(field_start - row);
    if (prefix_len >= new_row_size) return 0;
    memcpy(new_row, row, prefix_len);
    offset = prefix_len;
    if (!append_csv_field(new_row, new_row_size, &offset, set_value, 1)) return 0;
    if (*field_end == ',') {
        size_t suffix_len = strlen(field_end);
        if (offset + suffix_len >= new_row_size) return 0;
        memcpy(new_row + offset, field_end, suffix_len + 1);
    } else {
        if (offset >= new_row_size) return 0;
        new_row[offset] = '\0';
    }
    return 1;
}

static int materialize_cached_csv_rows(TableCache *tc) {
    char line[RECORD_SIZE];
    int slot_id = 0;

    if (!tc || tc->rows_materialized || tc->cache_truncated) return 1;
    if (!tc->file) return 0;
    if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_SET) != 0) return 0;
    if (!fgets(line, sizeof(line), tc->file)) return 0;

    while (slot_id < tc->record_count && fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;

        if (tc->record_active[slot_id] && tc->row_store[slot_id] == ROW_STORE_CSV && !tc->records[slot_id]) {
            tc->records[slot_id] = dup_string(line);
            if (!tc->records[slot_id]) return 0;
            tc->row_store[slot_id] = ROW_STORE_MEMORY;
            tc->row_offsets[slot_id] = -1;
            tc->row_cached[slot_id] = 0;
            tc->row_cache_seq[slot_id] = 0;
            if (tc->row_refs) {
                tc->row_refs[slot_id].store = ROW_STORE_MEMORY;
                tc->row_refs[slot_id].offset = -1;
            }
        }
        slot_id++;
    }
    if (fseek(tc->file, 0, SEEK_END) == 0) tc->append_offset = ftell(tc->file);
    tc->rows_materialized = 1;
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

static int maybe_rebuild_indexes_for_order(TableCache *tc) {
    int desired_order;
    int i;
    int needs_rebuild = 0;

    if (!tc || tc->active_count <= 0 || tc->cache_truncated) return 1;
    desired_order = bptree_recommended_order(tc->active_count);
    if (tc->id_index && bptree_order(tc->id_index) < desired_order) {
        needs_rebuild = 1;
    }
    for (i = 0; !needs_rebuild && i < tc->uk_count; i++) {
        if (tc->uk_indexes[i] &&
            bptree_string_order(tc->uk_indexes[i]->tree) < desired_order) {
            needs_rebuild = 1;
        }
    }
    if (!needs_rebuild) return 1;
    if (g_executor_quiet == 0) {
        INFO_PRINTF("[index] rebuilding PK/UK B+ trees for order %d at %d active rows.\n",
                    desired_order, tc->active_count);
    }
    return rebuild_id_index(tc) && rebuild_uk_indexes(tc);
}

typedef struct {
    long size;
    long mtime;
    int exists;
} FileStamp;

typedef struct SnapshotMeta {
    int snapshot_v2;
    int csv_exists;
    long csv_size;
    long csv_mtime;
    int delta_exists;
    long delta_size;
    long delta_mtime;
    int col_count;
    int pk_idx;
    int uk_count;
    int uk_indices[MAX_UKS];
    int record_count;
    int active_count;
    int cache_truncated;
    int tail_count;
    long next_auto_id;
    long next_row_id;
} SnapshotMeta;

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

static int write_index_snapshot_pairs(FILE *out, TableCache *tc) {
    BPlusPair *id_pairs = NULL;
    BPlusStringPair *uk_pairs[MAX_UKS] = {0};
    int uk_counts[MAX_UKS] = {0};
    int id_count = 0;
    int i;
    int row_index;

    if (tc->active_count > 0) {
        id_pairs = (BPlusPair *)calloc((size_t)tc->active_count, sizeof(BPlusPair));
        if (!id_pairs) return 0;
        for (i = 0; i < tc->uk_count; i++) {
            uk_pairs[i] = (BPlusStringPair *)calloc((size_t)tc->active_count, sizeof(BPlusStringPair));
            if (!uk_pairs[i]) goto fail;
        }
    }

    for (row_index = 0; row_index < tc->record_count; row_index++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};
        char *row = slot_row(tc, row_index);
        long id_value;

        if (!row) continue;
        if (!get_row_index_key(tc, row, row_index, &id_value)) goto fail;
        id_pairs[id_count].key = id_value;
        id_pairs[id_count].row_index = row_index;
        id_count++;

        parse_csv_row(row, fields, row_buf);
        for (i = 0; i < tc->uk_count; i++) {
            char key[RECORD_SIZE];
            int col_idx = tc->uk_indices[i];

            normalize_value(fields[col_idx], key, sizeof(key));
            if (strlen(key) == 0) continue;
            uk_pairs[i][uk_counts[i]].key = dup_string(key);
            if (!uk_pairs[i][uk_counts[i]].key) goto fail;
            uk_pairs[i][uk_counts[i]].row_index = row_index;
            uk_counts[i]++;
        }
    }

    if (id_count > 1) qsort(id_pairs, (size_t)id_count, sizeof(BPlusPair), compare_bplus_pair);
    if (fprintf(out, "ID %d\n", id_count) < 0) goto fail;
    for (i = 0; i < id_count; i++) {
        if (fprintf(out, "%ld\t%d\n", id_pairs[i].key, id_pairs[i].row_index) < 0) goto fail;
    }

    if (fprintf(out, "UKSECTIONS %d\n", tc->uk_count) < 0) goto fail;
    for (i = 0; i < tc->uk_count; i++) {
        int j;

        if (uk_counts[i] > 1) {
            qsort(uk_pairs[i], (size_t)uk_counts[i], sizeof(BPlusStringPair),
                  compare_bplus_string_pair);
        }
        if (fprintf(out, "UK %d %d\n", tc->uk_indices[i], uk_counts[i]) < 0) goto fail;
        for (j = 0; j < uk_counts[i]; j++) {
            if (fprintf(out, "%d\t%s\n", uk_pairs[i][j].row_index, uk_pairs[i][j].key) < 0) goto fail;
        }
    }

    free(id_pairs);
    for (i = 0; i < tc->uk_count; i++) {
        int j;
        for (j = 0; j < uk_counts[i]; j++) free(uk_pairs[i][j].key);
        free(uk_pairs[i]);
    }
    return 1;

fail:
    free(id_pairs);
    for (i = 0; i < tc->uk_count; i++) {
        int j;
        if (uk_pairs[i]) {
            for (j = 0; j < uk_counts[i]; j++) free(uk_pairs[i][j].key);
            free(uk_pairs[i]);
        }
    }
    return 0;
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

static int read_snapshot_meta(TableCache *tc, FILE *f, SnapshotMeta *meta,
                              int allow_v1, int require_delta_absent) {
    char filename[300];
    char csv_filename[300];
    char delta_filename[300];
    char line[DELTA_LINE_SIZE];
    FileStamp csv_stamp;
    FileStamp delta_stamp;
    int i;

    if (!tc || !f || !meta) return 0;
    memset(meta, 0, sizeof(*meta));

    snprintf(csv_filename, sizeof(csv_filename), "%s.csv", tc->table_name);
    get_delta_filename(tc, delta_filename, sizeof(delta_filename));
    csv_stamp = get_file_stamp(csv_filename);
    delta_stamp = get_file_stamp(delta_filename);
    if (require_delta_absent && delta_stamp.exists) return 0;

    if (!read_expected_line(f, line, sizeof(line))) return 0;
    if (strcmp(line, "SQLPROC_IDX_V2") == 0) {
        meta->snapshot_v2 = 1;
    } else if (allow_v1 && strcmp(line, "SQLPROC_IDX_V1") == 0) {
        meta->snapshot_v2 = 0;
    } else {
        return 0;
    }

    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "TABLE %255s", filename) != 1 ||
        strcmp(filename, tc->table_name) != 0) {
        return 0;
    }
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "CSV %d %ld %ld", &meta->csv_exists, &meta->csv_size, &meta->csv_mtime) != 3) {
        return 0;
    }
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "DELTA %d %ld %ld", &meta->delta_exists, &meta->delta_size, &meta->delta_mtime) != 3) {
        return 0;
    }
    if (meta->csv_exists != csv_stamp.exists ||
        meta->csv_size != csv_stamp.size ||
        meta->csv_mtime != csv_stamp.mtime ||
        meta->delta_exists != delta_stamp.exists ||
        meta->delta_size != delta_stamp.size ||
        meta->delta_mtime != delta_stamp.mtime) {
        return 0;
    }
    if (require_delta_absent && meta->delta_exists) return 0;

    if (!read_expected_line(f, line, sizeof(line))) return 0;
    {
        char *p = line;
        if (strncmp(p, "SCHEMA ", 7) != 0) return 0;
        p += 7;
        if (sscanf(p, "%d %d %d", &meta->col_count, &meta->pk_idx, &meta->uk_count) != 3) return 0;
        for (i = 0; i < 3; i++) {
            while (*p && !isspace((unsigned char)*p)) p++;
            while (isspace((unsigned char)*p)) p++;
        }
        for (i = 0; i < meta->uk_count && i < MAX_UKS; i++) {
            if (sscanf(p, "%d", &meta->uk_indices[i]) != 1) return 0;
            while (*p && !isspace((unsigned char)*p)) p++;
            while (isspace((unsigned char)*p)) p++;
        }
    }
    if (meta->col_count != tc->col_count || meta->pk_idx != tc->pk_idx || meta->uk_count != tc->uk_count) {
        return 0;
    }
    for (i = 0; i < meta->uk_count; i++) {
        if (meta->uk_indices[i] != tc->uk_indices[i]) return 0;
    }

    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "ROWS %d %d %d %d %ld %ld",
               &meta->record_count, &meta->active_count, &meta->cache_truncated,
               &meta->tail_count, &meta->next_auto_id, &meta->next_row_id) != 6) {
        return 0;
    }
    return 1;
}

static int read_snapshot_id_pairs(TableCache *tc, FILE *f, int active_count, BPlusPair **id_pairs_out) {
    char line[DELTA_LINE_SIZE];
    BPlusPair *id_pairs = NULL;
    int id_count;
    int i;

    if (!tc || !f || !id_pairs_out) return 0;
    *id_pairs_out = NULL;

    if (!read_expected_line(f, line, sizeof(line)) || sscanf(line, "ID %d", &id_count) != 1) return 0;
    if (id_count != active_count) return 0;
    if (id_count > 0) {
        id_pairs = (BPlusPair *)calloc((size_t)id_count, sizeof(BPlusPair));
        if (!id_pairs) return 0;
    }
    for (i = 0; i < id_count; i++) {
        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "%ld\t%d", &id_pairs[i].key, &id_pairs[i].row_index) != 2 ||
            !slot_is_active(tc, id_pairs[i].row_index)) {
            free(id_pairs);
            return 0;
        }
    }
    *id_pairs_out = id_pairs;
    return id_count;
}

static int read_snapshot_uk_indexes(TableCache *tc, FILE *f, int active_count,
                                    UniqueIndex *new_indexes[MAX_UKS]) {
    char line[DELTA_LINE_SIZE];
    int section_count;
    int i;

    if (!tc || !f || !new_indexes) return 0;
    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "UKSECTIONS %d", &section_count) != 1 ||
        section_count != tc->uk_count) {
        return 0;
    }

    for (i = 0; i < tc->uk_count; i++) {
        BPlusStringPair *pairs = NULL;
        int col_idx;
        int count;
        int j;

        if (!read_expected_line(f, line, sizeof(line)) ||
            sscanf(line, "UK %d %d", &col_idx, &count) != 2 ||
            col_idx != tc->uk_indices[i] || count < 0 || count > active_count) {
            return 0;
        }
        new_indexes[i] = unique_index_create(col_idx);
        if (!new_indexes[i]) return 0;
        if (count > 0) {
            pairs = (BPlusStringPair *)calloc((size_t)count, sizeof(BPlusStringPair));
            if (!pairs) return 0;
        }
        for (j = 0; j < count; j++) {
            char *tab;
            int row_index;

            if (!read_expected_line(f, line, sizeof(line))) {
                free(pairs);
                return 0;
            }
            tab = strchr(line, '\t');
            if (!tab) {
                free(pairs);
                return 0;
            }
            *tab++ = '\0';
            row_index = atoi(line);
            if (!slot_is_active(tc, row_index)) {
                free(pairs);
                return 0;
            }
            pairs[j].row_index = row_index;
            pairs[j].key = dup_string(tab);
            if (!pairs[j].key) {
                free(pairs);
                return 0;
            }
        }
        if (!bptree_string_build_from_sorted(new_indexes[i]->tree, pairs, count)) {
            for (j = 0; j < count; j++) free(pairs[j].key);
            free(pairs);
            return 0;
        }
        for (j = 0; j < count; j++) free(pairs[j].key);
        free(pairs);
    }

    return read_expected_line(f, line, sizeof(line)) && strcmp(line, "END") == 0;
}

static int load_index_snapshot(TableCache *tc) {
    char filename[300];
    char line[DELTA_LINE_SIZE];
    FILE *f;
    SnapshotMeta meta;
    int i;
    BPlusPair *id_pairs = NULL;
    UniqueIndex *new_indexes[MAX_UKS] = {0};
    int id_count;

    if (!tc || strlen(tc->table_name) == 0 || tc->cache_truncated) return 0;
    get_index_filename(tc, filename, sizeof(filename));
    f = fopen(filename, "r");
    if (!f) return 0;

    if (!read_snapshot_meta(tc, f, &meta, 1, 0)) goto fail;
    if (meta.record_count != tc->record_count || meta.active_count != tc->active_count ||
        meta.cache_truncated != tc->cache_truncated || meta.tail_count != tc->tail_count) {
        goto fail;
    }

    if (meta.snapshot_v2) {
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

    id_count = read_snapshot_id_pairs(tc, f, tc->active_count, &id_pairs);
    if (id_count < 0) goto fail;
    if (!read_snapshot_uk_indexes(tc, f, tc->active_count, new_indexes)) goto fail;

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
    tc->next_auto_id = meta.next_auto_id;
    tc->next_row_id = meta.next_row_id;
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
    char line[DELTA_LINE_SIZE];
    FILE *f;
    SnapshotMeta meta;
    int slot_count;
    int i;
    int active_seen = 0;
    BPlusPair *id_pairs = NULL;
    UniqueIndex *new_indexes[MAX_UKS] = {0};
    int id_count;

    if (!tc || strlen(tc->table_name) == 0) return 0;
    get_index_filename(tc, filename, sizeof(filename));
    f = fopen(filename, "r");
    if (!f) return 0;

    if (!read_snapshot_meta(tc, f, &meta, 0, 1)) goto fail;
    if (!meta.snapshot_v2 || meta.record_count < 0 || meta.record_count > MAX_RECORDS ||
        meta.active_count < 0 || meta.cache_truncated || meta.tail_count != 0) {
        goto fail;
    }
    if (!ensure_record_capacity(tc, meta.record_count)) goto fail;
    tc->record_count = meta.record_count;
    tc->active_count = 0;
    tc->cache_truncated = 0;
    tc->tail_count = 0;

    if (!read_expected_line(f, line, sizeof(line)) ||
        sscanf(line, "SLOTS %d", &slot_count) != 1 ||
        slot_count != meta.record_count) {
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
    if (active_seen != meta.active_count) goto fail;
    tc->active_count = active_seen;

    id_count = read_snapshot_id_pairs(tc, f, active_seen, &id_pairs);
    if (id_count < 0) goto fail;
    if (!bptree_build_from_sorted(tc->id_index, id_pairs, id_count)) goto fail;

    if (!read_snapshot_uk_indexes(tc, f, active_seen, new_indexes)) goto fail;

    for (i = 0; i < tc->uk_count; i++) {
        unique_index_destroy(tc->uk_indexes[i]);
        tc->uk_indexes[i] = new_indexes[i];
        new_indexes[i] = NULL;
    }
    tc->next_auto_id = meta.next_auto_id;
    tc->next_row_id = meta.next_row_id;
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

static int parse_table_header(TableCache *tc, FILE *f) {
    char header[RECORD_SIZE];

    if (!fgets(header, sizeof(header), f)) return 1;
    if ((unsigned char)header[0] == 0xEF &&
        (unsigned char)header[1] == 0xBB &&
        (unsigned char)header[2] == 0xBF) {
        memmove(header, header + 3, strlen(header + 3) + 1);
    }

    {
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
    }

    return ensure_uk_indexes(tc);
}

static int load_rows_into_cache(TableCache *tc, const char *name, FILE *f,
                                int has_delta_log, long *file_next_auto_id_out) {
    char line[RECORD_SIZE];
    long line_number = 1;
    long file_next_auto_id = 1;

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
    *file_next_auto_id_out = file_next_auto_id;
    return 1;
}

static int finalize_indexes_and_recovery(TableCache *tc, const char *name, FILE *f,
                                         int has_delta_log, long file_next_auto_id) {
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

static int load_table_contents(TableCache *tc, const char *name, FILE *f) {
    long file_next_auto_id = 1;
    int has_delta_log;

    reset_table_cache(tc);
    if (!tc->id_index) return 0;
    strncpy(tc->table_name, name, sizeof(tc->table_name) - 1);
    tc->file = f;
    touch_table(tc);
    has_delta_log = delta_log_exists(tc);

    if (!parse_table_header(tc, f)) return 0;
    if (load_table_parse_snapshot(tc)) return 1;
    if (!load_rows_into_cache(tc, name, f, has_delta_log, &file_next_auto_id)) return 0;
    return finalize_indexes_and_recovery(tc, name, f, has_delta_log, file_next_auto_id);
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
        if (!maybe_rebuild_indexes_for_order(tc)) {
            printf("[warning] INSERT completed, but dynamic B+ tree order rebuild failed.\n");
        }
    } else {
        long append_offset;
        int replacing_deleted_tail = 0;

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
        if (tc->pk_idx != -1) {
            tail_overlay_row(tc, id_value, &replacing_deleted_tail);
        }
        if (!append_tail_pk_offset(tc, tc->pk_idx != -1 ? id_value : tc->next_row_id++, append_offset)) {
            printf("[error] INSERT failed: could not update tail offset index.\n");
            return 0;
        }
        if (tc->pk_idx != -1) {
            if (replacing_deleted_tail) {
                if (!append_delta_update_key(tc, id_value, new_line) ||
                    !set_tail_delta(tc, id_value, new_line, 0)) {
                    printf("[error] INSERT failed: could not persist replacement for deleted tail PK.\n");
                    return 0;
                }
            } else {
                clear_tail_delta(tc, id_value);
            }
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

static int execute_insert_internal(Statement *stmt, long *inserted_id) {
    TableCache *tc = get_table(stmt->table_name);
    long id_value = 0;

    if (!tc) return 0;
    if (!insert_row_data(tc, stmt->row_data, 0, &id_value)) return 0;
    if (inserted_id) *inserted_id = id_value;
    return 1;
}

void execute_insert(Statement *stmt) {
    long id_value = 0;

    if (!execute_insert_internal(stmt, &id_value)) return;
    INFO_PRINTF("[ok] INSERT completed. id=%ld\n", id_value);
}

int execute_insert_values_fast(const char *table_name, const char *values_csv) {
    TableCache *tc;

    if (!table_name || !values_csv) return 0;
    tc = get_table(table_name);
    if (!tc) return 0;
    return insert_row_data(tc, values_csv, 0, NULL);
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

typedef struct {
    Statement *stmt;
    SelectExecContext *exec;
} SelectFileScanContext;

typedef struct {
    Statement *stmt;
    SelectExecContext *exec;
    int where_idx;
    long start_key;
    long end_key;
} SelectFileRangeContext;

typedef struct {
    Statement *stmt;
    SelectExecContext *exec;
    int where_idx;
    const char *start_key;
    const char *end_key;
} SelectFileStringRangeContext;

static void execute_select_file_scan(TableCache *tc, long start_offset, Statement *stmt,
                                     SelectExecContext *exec);
static int print_tail_pk_offset_row(TableCache *tc, long offset, long key,
                                    Statement *stmt, SelectExecContext *exec);
static void execute_select_file_range_scan(TableCache *tc, long start_offset, Statement *stmt,
                                           int where_idx, SelectExecContext *exec,
                                           long start_key, long end_key);
static void execute_select_file_string_range_scan(TableCache *tc, long start_offset, Statement *stmt,
                                                  int where_idx, SelectExecContext *exec,
                                                  const char *start_key, const char *end_key);

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

static const char *effective_tail_row(TableCache *tc, const char *line, int *skip) {
    long id_value;
    int deleted = 0;
    const char *overlay;

    if (skip) *skip = 0;
    if (!tc || !line || tc->pk_idx == -1 || !get_row_pk_value(tc, line, &id_value)) return line;
    overlay = tail_overlay_row(tc, id_value, &deleted);
    if (deleted) {
        if (skip) *skip = 1;
        return NULL;
    }
    return overlay ? overlay : line;
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

static void build_mutation_lookup_plan(TableCache *tc, Statement *stmt, MutationLookupPlan *plan) {
    int index_cond = -1;
    int index_col = -1;

    memset(plan, 0, sizeof(*plan));
    plan->condition_index = -1;
    plan->where_idx = -1;

    choose_index_condition(tc, stmt, 0, &index_cond, &index_col);
    plan->condition_index = index_cond;
    plan->where_idx = (index_col != -1)
        ? index_col
        : condition_column_index(tc, &stmt->where_conditions[0]);
    plan->uses_pk_lookup = (index_cond != -1 &&
                            index_col == tc->pk_idx &&
                            stmt->where_conditions[index_cond].type == WHERE_EQ);
    plan->uses_uk_lookup = (index_cond != -1 &&
                            !plan->uses_pk_lookup &&
                            get_uk_slot(tc, index_col) != -1 &&
                            stmt->where_conditions[index_cond].type == WHERE_EQ);
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

static int select_file_scan_visitor(TableCache *tc, const char *row, void *ctx) {
    SelectFileScanContext *scan_ctx = (SelectFileScanContext *)ctx;

    if (!scan_ctx || !scan_ctx->stmt || !scan_ctx->exec) return 0;
    if (!row_matches_statement(tc, scan_ctx->stmt, row)) return 1;
    emit_selected_row(row, scan_ctx->exec);
    return 1;
}

static int select_file_range_visitor(TableCache *tc, const char *row, void *ctx) {
    SelectFileRangeContext *range_ctx = (SelectFileRangeContext *)ctx;
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    long row_key;

    if (!range_ctx || !range_ctx->stmt || !range_ctx->exec) return 0;
    parse_csv_row(row, fields, row_buf);
    if (!parse_long_value(fields[range_ctx->where_idx], &row_key)) return 1;
    if (row_key < range_ctx->start_key || row_key > range_ctx->end_key) return 1;
    if (!row_fields_match_statement(tc, range_ctx->stmt, fields)) return 1;
    emit_selected_row(row, range_ctx->exec);
    return 1;
}

static int select_file_string_range_visitor(TableCache *tc, const char *row, void *ctx) {
    SelectFileStringRangeContext *range_ctx = (SelectFileStringRangeContext *)ctx;
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    char key[RECORD_SIZE];

    if (!range_ctx || !range_ctx->stmt || !range_ctx->exec) return 0;
    parse_csv_row(row, fields, row_buf);
    normalize_value(fields[range_ctx->where_idx], key, sizeof(key));
    if (strcmp(key, range_ctx->start_key) < 0 || strcmp(key, range_ctx->end_key) > 0) return 1;
    if (!row_fields_match_statement(tc, range_ctx->stmt, fields)) return 1;
    emit_selected_row(row, range_ctx->exec);
    return 1;
}

static int mark_selected_rows(TableCache *tc, Statement *stmt, int *match_flags, int *match_count) {
    int i;
    int count = 0;

    if (!tc || !stmt || !match_flags) return 0;

    for (i = 0; i < tc->record_count; i++) {
        char *row = slot_row(tc, i);

        if (!row) continue;
        if (!row_matches_statement(tc, stmt, row)) continue;
        match_flags[i] = 1;
        count++;
    }

    if (match_count) *match_count = count;
    return 1;
}

static int prepare_update_set(TableCache *tc, Statement *stmt, int *set_idx,
                              char *set_value, size_t set_value_size, int *rebuild_uk_needed) {
    int resolved_set_idx;

    if (!tc || !stmt || !set_idx || !set_value || set_value_size == 0 || !rebuild_uk_needed) return 0;

    resolved_set_idx = get_col_idx(tc, stmt->set_col);
    if (resolved_set_idx == -1) {
        printf("[error] UPDATE failed: WHERE or SET column does not exist.\n");
        return 0;
    }
    if (resolved_set_idx == tc->pk_idx) {
        printf("[error] UPDATE failed: PK column cannot be changed.\n");
        return 0;
    }

    strncpy(set_value, stmt->set_val, set_value_size - 1);
    set_value[set_value_size - 1] = '\0';
    trim_and_unquote(set_value);

    if (tc->cols[resolved_set_idx].type == COL_NN && strlen(set_value) == 0) {
        printf("[error] UPDATE failed: column '%s' violates NN constraint.\n", tc->cols[resolved_set_idx].name);
        return 0;
    }

    *set_idx = resolved_set_idx;
    *rebuild_uk_needed = (tc->cols[resolved_set_idx].type == COL_UK);
    return 1;
}

static int build_updated_row_copy(TableCache *tc, const char *row, int set_idx, const char *set_value,
                                  char **new_copy, int allow_slice_build) {
    char new_row[RECORD_SIZE];

    if (!tc || !row || !new_copy) return 0;

    if (allow_slice_build) {
        if (!build_updated_row_slice(tc, row, set_idx, set_value, new_row, sizeof(new_row)) &&
            !build_updated_row(tc, row, set_idx, set_value, new_row, sizeof(new_row))) {
            return 0;
        }
    } else if (!build_updated_row(tc, row, set_idx, set_value, new_row, sizeof(new_row))) {
        return 0;
    }

    *new_copy = dup_string(new_row);
    return *new_copy != NULL;
}

static int build_updated_row_text(TableCache *tc, const char *row, int set_idx, const char *set_value,
                                  char *new_row, size_t new_row_size) {
    char row_buf[RECORD_SIZE];
    char *fields[MAX_COLS] = {0};
    size_t offset = 0;
    int i;

    if (!tc || !row || !new_row || new_row_size == 0) return 0;

    new_row[0] = '\0';
    parse_csv_row(row, fields, row_buf);
    for (i = 0; i < tc->col_count; i++) {
        const char *value = (i == set_idx) ? set_value : (fields[i] ? fields[i] : "");
        if (!append_csv_field(new_row, new_row_size, &offset, value, i == tc->col_count - 1)) {
            return 0;
        }
    }
    return 1;
}

static int persist_updated_row(TableCache *tc, int target_row, char *old_record,
                               int set_idx, const char *set_value,
                               int rebuild_uk_needed, int allow_slice_build, int emit_logs) {
    char *new_copy;

    if (rebuild_uk_needed &&
        !validate_update_uk(tc, set_idx, set_value, target_row, NULL, 0)) {
        return -1;
    }
    if (!build_updated_row_copy(tc, old_record, set_idx, set_value, &new_copy, allow_slice_build)) {
        if (emit_logs) printf("[error] UPDATE failed: rebuilt row is too long.\n");
        return -1;
    }
    if (rebuild_uk_needed && !remove_record_single_uk(tc, old_record, set_idx)) {
        free(new_copy);
        if (emit_logs) printf("[error] UPDATE failed: UK index update failed; memory restored.\n");
        return -1;
    }

    set_slot_memory_row(tc, target_row, new_copy);
    if (rebuild_uk_needed && !index_record_single_uk(tc, target_row, set_idx)) {
        free(tc->records[target_row]);
        set_slot_memory_row(tc, target_row, old_record);
        rebuild_uk_indexes(tc);
        if (emit_logs) printf("[error] UPDATE failed: UK index update failed; memory restored.\n");
        return -1;
    }

    if (can_use_delta_log(tc)) {
        if (!append_delta_update_slot(tc, target_row, tc->records[target_row])) {
            free(tc->records[target_row]);
            set_slot_memory_row(tc, target_row, old_record);
            if (rebuild_uk_needed) rebuild_uk_indexes(tc);
            if (emit_logs) printf("[error] UPDATE failed: delta log append failed; memory restored.\n");
            return -1;
        }
        if (emit_logs) INFO_PRINTF("[delta] UPDATE persisted through append-only delta log.\n");
        if (!maybe_compact_delta_log(tc) && emit_logs) {
            printf("[warning] UPDATE completed, but delta compaction failed.\n");
        }
    } else if (!rewrite_file(tc)) {
        free(tc->records[target_row]);
        set_slot_memory_row(tc, target_row, old_record);
        if (rebuild_uk_needed) rebuild_uk_indexes(tc);
        if (emit_logs) printf("[error] UPDATE warning: memory changed but file rewrite failed.\n");
        return -1;
    }

    free(old_record);
    tc->snapshot_dirty = 1;
    if (emit_logs) INFO_PRINTF("[ok] UPDATE completed. rows=1\n");
    return 1;
}

static void set_slot_memory_row(TableCache *tc, int slot_id, char *row) {
    if (!tc || slot_id < 0 || slot_id >= tc->record_count) return;

    if (tc->row_cached[slot_id] && tc->cached_record_count > 0) {
        tc->cached_record_count--;
    }
    tc->records[slot_id] = row;
    tc->row_store[slot_id] = ROW_STORE_MEMORY;
    tc->row_offsets[slot_id] = -1;
    if (tc->row_refs) {
        tc->row_refs[slot_id].store = ROW_STORE_MEMORY;
        tc->row_refs[slot_id].offset = -1;
    }
    tc->row_cached[slot_id] = 0;
    tc->row_cache_seq[slot_id] = 0;
}

static int validate_update_uk(TableCache *tc, int set_idx, const char *set_value,
                              int target_row, int *match_flags, int target_count) {
    int found_row = -1;
    int uk_slot = get_uk_slot(tc, set_idx);

    if (match_flags && target_count > 1) {
        printf("[error] UPDATE failed: multiple rows would share one UK value.\n");
        return 0;
    }
    if (uk_slot == -1 || !ensure_uk_indexes(tc)) {
        printf("[error] UPDATE failed: UK index is not available.\n");
        return 0;
    }
    if (strlen(set_value) == 0) return 1;
    if (unique_index_find(tc, tc->uk_indexes[uk_slot], set_value, &found_row)) {
        if (!match_flags && found_row != target_row) {
            printf("[error] UPDATE failed: '%s' violates UK constraint.\n", set_value);
            return 0;
        }
        if (match_flags && (found_row < 0 || !match_flags[found_row])) {
            printf("[error] UPDATE failed: '%s' violates UK constraint.\n", set_value);
            return 0;
        }
    }
    return 1;
}

static int execute_update_scan(TableCache *tc, Statement *stmt, int set_idx, const char *set_value,
                               int rebuild_uk_needed) {
    int *match_flags;
    char **old_records;
    int target_count = 0;
    int count = 0;
    int i;

    match_flags = (int *)calloc((size_t)tc->record_count, sizeof(int));
    if (!match_flags) {
        printf("[error] UPDATE failed: out of memory.\n");
        return -1;
    }
    old_records = (char **)calloc((size_t)tc->record_count, sizeof(char *));
    if (!old_records) {
        free(match_flags);
        printf("[error] UPDATE failed: out of memory.\n");
        return -1;
    }

    mark_selected_rows(tc, stmt, match_flags, &target_count);
    if (target_count == 0) {
        free(old_records);
        free(match_flags);
        return 0;
    }

    if (rebuild_uk_needed &&
        !validate_update_uk(tc, set_idx, set_value, -1, match_flags, target_count)) {
        free(old_records);
        free(match_flags);
        return -1;
    }

    for (i = 0; i < tc->record_count; i++) {
        char *row;
        char *new_copy;

        if (!match_flags[i]) continue;
        row = slot_row(tc, i);
        if (!row) continue;
        if (!build_updated_row_copy(tc, row, set_idx, set_value, &new_copy, 0)) {
            rollback_updated_records(tc, old_records);
            free(old_records);
            free(match_flags);
            printf("[error] UPDATE failed: rebuilt row is too long.\n");
            return -1;
        }

        old_records[i] = tc->records[i];
        set_slot_memory_row(tc, i, new_copy);
        count++;
    }

    free(match_flags);
    if (rebuild_uk_needed) {
        for (i = 0; i < tc->record_count; i++) {
            if (!old_records[i]) continue;
            if (!remove_record_single_uk(tc, old_records[i], set_idx) ||
                !index_record_single_uk(tc, i, set_idx)) {
                rollback_updated_records(tc, old_records);
                rebuild_uk_indexes(tc);
                free(old_records);
                printf("[error] UPDATE failed: UK index update failed; memory restored.\n");
                return -1;
            }
        }
    }

    if (can_use_delta_log(tc)) {
        if (!append_delta_updates(tc, old_records)) {
            rollback_updated_records(tc, old_records);
            if (rebuild_uk_needed) rebuild_uk_indexes(tc);
            free(old_records);
            printf("[error] UPDATE failed: delta log append failed; memory restored.\n");
            return -1;
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
        return -1;
    }

    for (i = 0; i < tc->record_count; i++) free(old_records[i]);
    free(old_records);
    tc->snapshot_dirty = 1;
    INFO_PRINTF("[ok] UPDATE completed. rows=%d\n", count);
    return 1;
}

static int open_rewrite_output(TableCache *tc, char *filename, size_t filename_size,
                               char *temp_filename, size_t temp_filename_size, FILE **out) {
    if (!tc || !filename || !temp_filename || !out) return 0;

    snprintf(filename, filename_size, "%s.csv", tc->table_name);
    snprintf(temp_filename, temp_filename_size, "%s.tmp", filename);
    *out = fopen(temp_filename, "wb");
    if (!*out) return 0;
    if (!write_table_header(*out, tc)) {
        fclose(*out);
        remove(temp_filename);
        *out = NULL;
        return 0;
    }
    return 1;
}

static int commit_rewrite_output(TableCache *tc, FILE *out,
                                 const char *filename, const char *temp_filename) {
    if (!tc || !out || !filename || !temp_filename) return 0;

    if (fflush(out) != 0 || ferror(out)) return 0;
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
    return reload_table_cache(tc);
}

static int abort_rewrite_output(TableCache *tc, FILE *out, const char *temp_filename) {
    if (out) fclose(out);
    if (temp_filename) remove(temp_filename);
    if (tc && tc->file) fseek(tc->file, 0, SEEK_END);
    return 0;
}

static int scan_truncated_update_targets(TableCache *tc, Statement *stmt, int set_idx,
                                         const char *set_value, int *target_count, int *uk_conflict) {
    char line[RECORD_SIZE];

    if (target_count) *target_count = 0;
    if (uk_conflict) *uk_conflict = 0;
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
        if (matched) {
            if (target_count) (*target_count)++;
            continue;
        }
        if (uk_conflict &&
            tc->cols[set_idx].type == COL_UK &&
            strlen(set_value) > 0 &&
            compare_value(fields[set_idx], set_value)) {
            *uk_conflict = 1;
        }
    }
    return 1;
}

static int execute_delete_single_row(TableCache *tc, Statement *stmt, int index_col,
                                     const WhereCondition *lookup_cond, int uses_pk_lookup) {
    int target_row = -1;
    char *old_record;

    if (uses_pk_lookup) {
        INFO_PRINTF("[index] B+ tree id lookup for DELETE\n");
        if (!find_pk_row(tc, lookup_cond->val, &target_row)) return 0;
    } else {
        INFO_PRINTF("[index] UK B+ tree lookup for DELETE on column '%s'\n", lookup_cond->col);
        if (!find_uk_row(tc, index_col, lookup_cond->val, &target_row)) return 0;
    }

    old_record = slot_row(tc, target_row);
    if (!old_record) {
        printf("[error] DELETE failed: target row could not be loaded.\n");
        return -1;
    }
    if (!row_matches_statement(tc, stmt, old_record)) return 0;
    return persist_deleted_slot(tc, target_row, old_record, 1);
}

static int execute_delete_scan(TableCache *tc, Statement *stmt) {
    int old_count;
    char **old_records;
    int *delete_flags;
    int count = 0;
    int read_idx;

    old_count = tc->record_count;
    if (old_count == 0) return 0;

    old_records = (char **)malloc((size_t)old_count * sizeof(char *));
    delete_flags = (int *)calloc((size_t)old_count, sizeof(int));
    if (!old_records || !delete_flags) {
        free(old_records);
        free(delete_flags);
        printf("[error] DELETE failed: out of memory.\n");
        return -1;
    }

    for (read_idx = 0; read_idx < old_count; read_idx++) {
        old_records[read_idx] = slot_is_active(tc, read_idx) ? slot_row(tc, read_idx) : NULL;
    }
    mark_selected_rows(tc, stmt, delete_flags, &count);
    if (count == 0) {
        free(old_records);
        free(delete_flags);
        return 0;
    }

    for (read_idx = 0; read_idx < tc->record_count; read_idx++) {
        if (delete_flags[read_idx] && tc->record_active[read_idx]) {
            tc->record_active[read_idx] = 0;
            tc->active_count--;
        }
    }

    if (can_use_delta_log(tc)) {
        if (!append_delta_deletes(tc, old_records, delete_flags, old_count)) {
            reset_deleted_flags(tc, delete_flags, old_count);
            tc->active_count += count;
            free(old_records);
            free(delete_flags);
            printf("[error] DELETE failed: delta log append failed; memory restored.\n");
            return -1;
        }
        INFO_PRINTF("[delta] DELETE persisted through append-only delta log.\n");
        if (!maybe_compact_delta_log(tc)) {
            printf("[warning] DELETE completed, but delta compaction failed.\n");
        }
    } else if (!rewrite_file(tc)) {
        reset_deleted_flags(tc, delete_flags, old_count);
        tc->active_count += count;
        free(old_records);
        free(delete_flags);
        printf("[error] DELETE warning: memory changed but file rewrite failed.\n");
        return -1;
    }

    release_deleted_slots(tc, delete_flags, old_count);
    free(old_records);
    free(delete_flags);
    tc->snapshot_dirty = 1;
    INFO_PRINTF("[ok] DELETE completed. rows=%d\n", count);
    return 1;
}

static int resolve_tail_pk_row(TableCache *tc, Statement *stmt, long pk_key, long offset,
                               char *base_row, size_t base_row_size, const char **current_row) {
    const char *overlay;
    int deleted = 0;
    long actual_key;

    if (!tc || !stmt || !base_row || !current_row) return -1;

    overlay = tail_overlay_row(tc, pk_key, &deleted);
    if (deleted) return 0;
    if (overlay) {
        *current_row = overlay;
    } else {
        if (!read_csv_row_at_offset(tc, offset, base_row, base_row_size)) return -1;
        if (!get_row_pk_value(tc, base_row, &actual_key) || actual_key != pk_key) return 0;
        *current_row = base_row;
    }
    if (!row_matches_statement(tc, stmt, *current_row)) return 0;
    return 1;
}

static int persist_deleted_slot(TableCache *tc, int target_row, char *old_record, int emit_logs) {
    if (!tc || target_row < 0 || target_row >= tc->record_count || !old_record) return -1;
    if (!ensure_free_slot_capacity(tc, tc->free_count + 1)) {
        if (emit_logs) printf("[error] DELETE failed: out of memory.\n");
        return -1;
    }

    tc->record_active[target_row] = 0;
    tc->active_count--;
    if (can_use_delta_log(tc)) {
        if (!append_delta_delete_slot(tc, target_row, old_record)) {
            tc->record_active[target_row] = 1;
            tc->active_count++;
            if (emit_logs) printf("[error] DELETE failed: delta log append failed; memory restored.\n");
            return -1;
        }
        if (emit_logs) INFO_PRINTF("[delta] DELETE persisted through append-only delta log.\n");
        if (!maybe_compact_delta_log(tc) && emit_logs) {
            printf("[warning] DELETE completed, but delta compaction failed.\n");
        }
    } else if (!rewrite_file(tc)) {
        tc->record_active[target_row] = 1;
        tc->active_count++;
        if (emit_logs) printf("[error] DELETE warning: memory changed but file rewrite failed.\n");
        return -1;
    }

    clear_slot_storage(tc, target_row);
    tc->free_slots[tc->free_count++] = target_row;
    tc->snapshot_dirty = 1;
    if (emit_logs) INFO_PRINTF("[ok] DELETE completed. rows=1\n");
    return 1;
}

static int scan_select_file_rows(TableCache *tc, long start_offset,
                                 const char *tail_trace, const char *full_trace,
                                 int (*visitor)(TableCache *, const char *, void *), void *ctx) {
    char line[RECORD_SIZE];

    if (!tc || !tc->file || !visitor) return 0;
    if (fflush(tc->file) != 0) return 0;

    if (start_offset > 0) {
        if (fseek(tc->file, start_offset, SEEK_SET) != 0) return 0;
        if (tail_trace) printf(tail_trace, start_offset);
    } else {
        if (fseek(tc->file, 0, SEEK_SET) != 0) return 0;
        if (!fgets(line, sizeof(line), tc->file)) {
            fseek(tc->file, 0, SEEK_END);
            return 1;
        }
        if (full_trace) printf("%s", full_trace);
    }

    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        size_t line_len = strlen(line);
        int skip = 0;
        const char *effective;

        if (!nl && line_len == sizeof(line) - 1 && !feof(tc->file)) {
            printf("[error] row too long while scanning '%s' (max %d bytes).\n",
                   tc->table_name, RECORD_SIZE - 1);
            fseek(tc->file, 0, SEEK_END);
            return 0;
        }
        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;

        effective = effective_tail_row(tc, line, &skip);
        if (skip || !effective) continue;
        if (!visitor(tc, effective, ctx)) {
            fseek(tc->file, 0, SEEK_END);
            return 0;
        }
    }

    fseek(tc->file, 0, SEEK_END);
    return 1;
}

static void reset_deleted_flags(TableCache *tc, int *delete_flags, int old_count) {
    int i;

    if (!tc || !delete_flags) return;
    for (i = 0; i < old_count; i++) {
        if (!delete_flags[i]) continue;
        tc->record_active[i] = 1;
    }
}

static void release_deleted_slots(TableCache *tc, int *delete_flags, int old_count) {
    int i;

    if (!tc || !delete_flags) return;
    for (i = 0; i < old_count; i++) {
        if (!delete_flags[i]) continue;
        clear_slot_storage(tc, i);
        push_free_slot(tc, i);
    }
}

static void set_select_match_count(int *matched_rows, SelectExecContext *exec) {
    if (matched_rows) *matched_rows = exec->matched_rows;
}

static int select_can_short_circuit(Statement *stmt, SelectExecContext *exec, int *matched_rows) {
    if (exec->emit_results || stmt->where_count != 1) return 0;
    if (matched_rows) *matched_rows = 1;
    return 1;
}

static int emit_indexed_row_if_needed(TableCache *tc, Statement *stmt, SelectExecContext *exec, int row_index) {
    char *row;

    row = slot_row(tc, row_index);
    if (row && row_matches_statement(tc, stmt, row)) {
        emit_selected_row(row, exec);
    }
    return 1;
}

static int execute_select_pk_range(TableCache *tc, Statement *stmt, WhereCondition *cond,
                                   int where_idx, SelectExecContext *exec, int *matched_rows) {
    long start_key;
    long end_key;
    RangePrintContext range_ctx;

    if (!parse_long_value(cond->val, &start_key) ||
        !parse_long_value(cond->end_val, &end_key)) {
        printf("[error] SELECT failed: BETWEEN bounds must be integers for PK range search.\n");
        return 0;
    }

    range_ctx.tc = tc;
    range_ctx.stmt = stmt;
    range_ctx.exec = exec;

    if (exec->emit_traces) INFO_PRINTF("[index] B+ tree id range lookup\n");
    if (!bptree_range_search(tc->id_index, start_key, end_key, print_range_row_visitor, &range_ctx)) {
        printf("[error] SELECT failed: B+ tree range scan failed.\n");
        return 0;
    }
    if (tc->cache_truncated) {
        execute_select_file_range_scan(tc, tc->uncached_start_offset, stmt, where_idx, exec,
                                       start_key, end_key);
    }
    set_select_match_count(matched_rows, exec);
    return 1;
}

static int execute_select_uk_range(TableCache *tc, Statement *stmt, WhereCondition *cond,
                                   int where_idx, SelectExecContext *exec, int *matched_rows) {
    RangePrintContext range_ctx;
    char start_text[RECORD_SIZE];
    char end_text[RECORD_SIZE];
    int uk_slot = get_uk_slot(tc, where_idx);

    if (uk_slot == -1 || !ensure_uk_indexes(tc)) {
        printf("[error] SELECT failed: UK index is not available.\n");
        return 0;
    }

    normalize_value(cond->val, start_text, sizeof(start_text));
    normalize_value(cond->end_val, end_text, sizeof(end_text));

    range_ctx.tc = tc;
    range_ctx.stmt = stmt;
    range_ctx.exec = exec;

    if (exec->emit_traces) INFO_PRINTF("[index] UK B+ tree range lookup on column '%s'\n", cond->col);
    if (!bptree_string_range_search(tc->uk_indexes[uk_slot]->tree, start_text, end_text,
                                    print_string_range_row_visitor, &range_ctx)) {
        printf("[error] SELECT failed: UK B+ tree range scan failed.\n");
        return 0;
    }
    if (tc->cache_truncated) {
        execute_select_file_string_range_scan(tc, tc->uncached_start_offset, stmt, where_idx, exec,
                                              start_text, end_text);
    }
    set_select_match_count(matched_rows, exec);
    return 1;
}

static int execute_select_pk_eq(TableCache *tc, Statement *stmt, WhereCondition *cond,
                                SelectExecContext *exec, int *matched_rows) {
    long key;
    int row_index;

    if (!parse_long_value(cond->val, &key)) {
        printf("[error] SELECT failed: id condition must be an integer.\n");
        return 0;
    }

    if (exec->emit_traces) INFO_PRINTF("[index] B+ tree id lookup\n");
    if (bptree_search(tc->id_index, key, &row_index)) {
        if (select_can_short_circuit(stmt, exec, matched_rows)) return 1;
        emit_indexed_row_if_needed(tc, stmt, exec, row_index);
        set_select_match_count(matched_rows, exec);
        return 1;
    }

    if (tc->cache_truncated) {
        long tail_offset;

        if (find_tail_pk_offset(tc, key, &tail_offset)) {
            int deleted = 0;
            tail_overlay_row(tc, key, &deleted);
            if (deleted) {
                if (matched_rows) *matched_rows = 0;
                return 1;
            }
            if (select_can_short_circuit(stmt, exec, matched_rows)) return 1;
            print_tail_pk_offset_row(tc, tail_offset, key, stmt, exec);
            set_select_match_count(matched_rows, exec);
            return 1;
        }
        execute_select_file_scan(tc, tc->uncached_start_offset, stmt, exec);
    }

    set_select_match_count(matched_rows, exec);
    return 1;
}

static int execute_select_uk_eq(TableCache *tc, Statement *stmt, int index_col, WhereCondition *cond,
                                SelectExecContext *exec, int *matched_rows) {
    int row_index;

    if (exec->emit_traces) INFO_PRINTF("[index] UK B+ tree lookup on column '%s'\n", cond->col);
    if (find_uk_row(tc, index_col, cond->val, &row_index)) {
        if (select_can_short_circuit(stmt, exec, matched_rows)) return 1;
        emit_indexed_row_if_needed(tc, stmt, exec, row_index);
        set_select_match_count(matched_rows, exec);
        return 1;
    }

    if (tc->cache_truncated) {
        execute_select_file_scan(tc, tc->uncached_start_offset, stmt, exec);
    }

    set_select_match_count(matched_rows, exec);
    return 1;
}

static void execute_select_linear_scan(TableCache *tc, Statement *stmt, SelectExecContext *exec) {
    int i;

    if (stmt->where_count == 1 && exec->emit_traces) {
        printf("[scan] linear scan on column '%s'\n", stmt->where_conditions[0].col);
    } else if (stmt->where_count > 1 && exec->emit_traces) {
        printf("[scan] linear scan with %d WHERE condition(s)\n", stmt->where_count);
    }

    for (i = 0; i < tc->record_count; i++) {
        int owned = 0;
        char *row = slot_row_scan(tc, i, &owned);
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};

        if (!row) continue;
        parse_csv_row(row, fields, row_buf);
        if (!row_fields_match_statement(tc, stmt, fields)) {
            if (owned) free(row);
            continue;
        }
        emit_selected_row(row, exec);
        if (owned) free(row);
    }

    if (tc->cache_truncated) {
        execute_select_file_scan(tc, tc->uncached_start_offset, stmt, exec);
    }
}

static void execute_select_file_scan(TableCache *tc, long start_offset, Statement *stmt,
                                     SelectExecContext *exec) {
    SelectFileScanContext scan_ctx;
    const char *tail_trace = NULL;
    const char *full_trace = NULL;

    if (!tc || !tc->file || !stmt || !exec) return;
    scan_ctx.stmt = stmt;
    scan_ctx.exec = exec;
    if (exec->emit_traces) {
        tail_trace = "[scan] uncached CSV tail scan from offset %ld\n";
        full_trace = "[scan] full CSV scan\n";
    }
    if (!scan_select_file_rows(tc, start_offset, tail_trace, full_trace,
                               select_file_scan_visitor, &scan_ctx)) {
        printf("[error] SELECT failed: could not scan table file.\n");
    }
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
    {
        int deleted = 0;
        const char *overlay = tail_overlay_row(tc, key, &deleted);
        const char *effective = overlay ? overlay : line;

        if (deleted || !row_matches_statement(tc, stmt, effective)) {
            fseek(tc->file, 0, SEEK_END);
            return 0;
        }
        if (exec->emit_traces) INFO_PRINTF("[index] tail PK offset lookup\n");
        emit_selected_row(effective, exec);
    }
    fseek(tc->file, 0, SEEK_END);
    return 1;
}

static void execute_select_file_range_scan(TableCache *tc, long start_offset, Statement *stmt,
                                           int where_idx, SelectExecContext *exec,
                                           long start_key, long end_key) {
    SelectFileRangeContext range_ctx;
    const char *tail_trace = NULL;
    const char *full_trace = NULL;

    if (!tc || !tc->file || !stmt || !exec || start_key > end_key) return;
    range_ctx.stmt = stmt;
    range_ctx.exec = exec;
    range_ctx.where_idx = where_idx;
    range_ctx.start_key = start_key;
    range_ctx.end_key = end_key;
    if (exec->emit_traces) {
        tail_trace = "[scan] uncached CSV tail range scan from offset %ld\n";
        full_trace = "[scan] full CSV range scan\n";
    }
    if (!scan_select_file_rows(tc, start_offset, tail_trace, full_trace,
                               select_file_range_visitor, &range_ctx)) {
        printf("[error] SELECT failed: could not scan table file.\n");
    }
}

static void execute_select_file_string_range_scan(TableCache *tc, long start_offset, Statement *stmt,
                                                  int where_idx, SelectExecContext *exec,
                                                  const char *start_key, const char *end_key) {
    SelectFileStringRangeContext range_ctx;
    const char *tail_trace = NULL;
    const char *full_trace = NULL;

    if (!tc || !tc->file || !stmt || !exec || !start_key || !end_key ||
        strcmp(start_key, end_key) > 0) {
        return;
    }
    range_ctx.stmt = stmt;
    range_ctx.exec = exec;
    range_ctx.where_idx = where_idx;
    range_ctx.start_key = start_key;
    range_ctx.end_key = end_key;
    if (exec->emit_traces) {
        tail_trace = "[scan] uncached CSV tail string range scan from offset %ld\n";
        full_trace = "[scan] full CSV string range scan\n";
    }
    if (!scan_select_file_rows(tc, start_offset, tail_trace, full_trace,
                               select_file_string_range_visitor, &range_ctx)) {
        printf("[error] SELECT failed: could not scan table file.\n");
    }
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
        if (index_col == tc->pk_idx) {
            return execute_select_pk_range(tc, stmt, cond, index_col, &exec, matched_rows);
        }
        if (get_uk_slot(tc, index_col) != -1) {
            return execute_select_uk_range(tc, stmt, cond, index_col, &exec, matched_rows);
        }
        printf("[error] SELECT failed: BETWEEN uses PK or UK B+ tree indexes only.\n");
        return 0;
    }

    if (index_cond != -1 && stmt->where_conditions[index_cond].type == WHERE_EQ &&
        index_col == tc->pk_idx && index_col != -1) {
        return execute_select_pk_eq(tc, stmt, &stmt->where_conditions[index_cond], &exec, matched_rows);
    }

    if (index_cond != -1 && stmt->where_conditions[index_cond].type == WHERE_EQ &&
        index_col != -1 && get_uk_slot(tc, index_col) != -1) {
        return execute_select_uk_eq(tc, stmt, index_col, &stmt->where_conditions[index_cond],
                                    &exec, matched_rows);
    }

    execute_select_linear_scan(tc, stmt, &exec);
    set_select_match_count(matched_rows, &exec);
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
    FILE *out = NULL;
    int count = 0;
    int target_count = 0;
    int uk_conflict = 0;

    if (!tc || !tc->file) return 0;
    if (!scan_truncated_update_targets(tc, stmt, set_idx, set_value, &target_count, &uk_conflict)) return 0;
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

    if (!open_rewrite_output(tc, filename, sizeof(filename), temp_filename, sizeof(temp_filename), &out)) {
        return 0;
    }

    if (fseek(tc->file, 0, SEEK_SET) != 0) goto fail;
    if (!fgets(line, sizeof(line), tc->file)) goto fail;
    while (fgets(line, sizeof(line), tc->file)) {
        char *nl = strpbrk(line, "\r\n");
        int matched;

        if (nl) *nl = '\0';
        if (strlen(line) == 0) continue;
        matched = row_matches_statement(tc, stmt, line);
        if (matched) {
            char new_row[RECORD_SIZE];

            if (!build_updated_row_text(tc, line, set_idx, set_value, new_row, sizeof(new_row))) goto fail;
            if (fprintf(out, "%s\n", new_row) < 0) goto fail;
            count++;
            continue;
        }
        if (fprintf(out, "%s\n", line) < 0) goto fail;
    }
    if (!commit_rewrite_output(tc, out, filename, temp_filename)) return 0;
    INFO_PRINTF("[ok] UPDATE completed with CSV scan fallback. rows=%d\n", count);
    return 1;

fail:
    return abort_rewrite_output(tc, out, temp_filename);
}

static int rewrite_truncated_delete(TableCache *tc, Statement *stmt) {
    char filename[300];
    char temp_filename[320];
    char line[RECORD_SIZE];
    FILE *out = NULL;
    int count = 0;

    if (!tc || !tc->file) return 0;
    if (!open_rewrite_output(tc, filename, sizeof(filename), temp_filename, sizeof(temp_filename), &out)) {
        return 0;
    }

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
    if (count == 0) {
        abort_rewrite_output(tc, out, temp_filename);
        INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
        return 1;
    }
    if (!commit_rewrite_output(tc, out, filename, temp_filename)) return 0;
    INFO_PRINTF("[ok] DELETE completed with CSV scan fallback. rows=%d\n", count);
    return 1;

fail:
    return abort_rewrite_output(tc, out, temp_filename);
}

static int read_csv_row_at_offset(TableCache *tc, long offset, char *line, size_t line_size) {
    char *nl;
    size_t line_len;

    if (!tc || !tc->file || !line || line_size == 0 || offset < 0) return 0;
    if (fflush(tc->file) != 0 || fseek(tc->file, offset, SEEK_SET) != 0) return 0;
    if (!fgets(line, (int)line_size, tc->file)) {
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }
    nl = strpbrk(line, "\r\n");
    line_len = strlen(line);
    if (!nl && line_len == line_size - 1 && !feof(tc->file)) {
        fseek(tc->file, 0, SEEK_END);
        return 0;
    }
    if (nl) *nl = '\0';
    fseek(tc->file, 0, SEEK_END);
    return strlen(line) > 0;
}

static int execute_update_tail_pk_row(TableCache *tc, Statement *stmt, long pk_key, long offset,
                                      int set_idx, const char *set_value) {
    char base_row[RECORD_SIZE];
    const char *current_row;
    char new_row[RECORD_SIZE];
    int resolved = resolve_tail_pk_row(tc, stmt, pk_key, offset, base_row, sizeof(base_row), &current_row);

    if (resolved < 0) {
        printf("[error] UPDATE failed: could not read uncached CSV tail row.\n");
        return -1;
    }
    if (resolved == 0) return 0;
    if (!build_updated_row_text(tc, current_row, set_idx, set_value, new_row, sizeof(new_row))) {
        printf("[error] UPDATE failed: rebuilt row is too long.\n");
        return -1;
    }
    if (!append_delta_update_key(tc, pk_key, new_row)) {
        printf("[error] UPDATE failed: delta log append failed.\n");
        return -1;
    }
    if (!set_tail_delta(tc, pk_key, new_row, 0)) {
        printf("[error] UPDATE failed: out of memory while caching tail delta.\n");
        return -1;
    }
    tc->snapshot_dirty = 1;
    INFO_PRINTF("[delta] UPDATE persisted through append-only delta log for uncached tail row.\n");
    INFO_PRINTF("[ok] UPDATE completed. rows=1\n");
    return 1;
}

static int execute_delete_tail_pk_row(TableCache *tc, Statement *stmt, long pk_key, long offset) {
    char base_row[RECORD_SIZE];
    const char *current_row;
    int resolved = resolve_tail_pk_row(tc, stmt, pk_key, offset, base_row, sizeof(base_row), &current_row);

    if (resolved < 0) {
        printf("[error] DELETE failed: could not read uncached CSV tail row.\n");
        return -1;
    }
    if (resolved == 0) return 0;
    if (!append_delta_delete_key(tc, pk_key)) {
        printf("[error] DELETE failed: delta log append failed.\n");
        return -1;
    }
    if (!set_tail_delta(tc, pk_key, NULL, 1)) {
        printf("[error] DELETE failed: out of memory while caching tail delta.\n");
        return -1;
    }
    tc->snapshot_dirty = 1;
    INFO_PRINTF("[delta] DELETE persisted through append-only delta log for uncached tail row.\n");
    INFO_PRINTF("[ok] DELETE completed. rows=1\n");
    return 1;
}

static int execute_update_single_row(TableCache *tc, Statement *stmt, int where_idx,
                                     const WhereCondition *lookup_cond, int set_idx, const char *set_value, int uses_pk_lookup,
                                     int uses_uk_lookup, int rebuild_uk_needed) {
    int target_row = -1;
    char *old_record;

    if (uses_pk_lookup) {
        INFO_PRINTF("[index] B+ tree id lookup for UPDATE\n");
        if (!find_pk_row(tc, lookup_cond->val, &target_row)) return 0;
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
    if (!row_matches_statement(tc, stmt, old_record)) return 0;
    return persist_updated_row(tc, target_row, old_record, set_idx, set_value,
                               rebuild_uk_needed, 0, 1);
}

static int execute_update_internal(Statement *stmt, int *affected_rows) {
    TableCache *tc = get_table(stmt->table_name);
    MutationLookupPlan lookup;
    int set_idx;
    char set_value[256];
    int rebuild_uk_needed;
    int result;

    if (affected_rows) *affected_rows = 0;
    if (!tc) return 0;
    if (!validate_where_columns(tc, stmt, "UPDATE")) return 0;
    if (stmt->where_count == 0) {
        printf("[error] UPDATE failed: WHERE condition is required.\n");
        return 0;
    }
    build_mutation_lookup_plan(tc, stmt, &lookup);
    if (lookup.where_idx == -1) {
        printf("[error] UPDATE failed: WHERE or SET column does not exist.\n");
        return 0;
    }
    if (!prepare_update_set(tc, stmt, &set_idx, set_value, sizeof(set_value), &rebuild_uk_needed)) {
        return 0;
    }

    if (tc->cache_truncated) {
        if (lookup.uses_pk_lookup && stmt->where_count == 1 && !rebuild_uk_needed) {
            long pk_key;
            long tail_offset;

            if (!parse_long_value(stmt->where_conditions[lookup.condition_index].val, &pk_key)) {
                return 1;
            }
            if (find_tail_pk_offset(tc, pk_key, &tail_offset)) {
                int result = execute_update_tail_pk_row(tc, stmt, pk_key, tail_offset, set_idx, set_value);
                if (result > 0 && affected_rows) *affected_rows = result;
                return result >= 0;
            }
            {
                int result = execute_update_single_row(tc, stmt, lookup.where_idx, &stmt->where_conditions[lookup.condition_index],
                                                       set_idx, set_value,
                                                       lookup.uses_pk_lookup, lookup.uses_uk_lookup, rebuild_uk_needed);
                if (result > 0 && affected_rows) *affected_rows = result;
                return result >= 0;
            }
        }
        if (lookup.uses_uk_lookup) {
            INFO_PRINTF("[notice] UPDATE on uncached table uses CSV scan fallback because tail rows have no UK offset index.\n");
        }
        if (!rewrite_truncated_update(tc, stmt, set_idx, set_value)) {
            printf("[error] UPDATE failed while using CSV scan fallback.\n");
            return 0;
        }
        if (affected_rows) *affected_rows = 1;
        return 1;
    }

    if (lookup.uses_pk_lookup || lookup.uses_uk_lookup) {
        result = execute_update_single_row(tc, stmt, lookup.where_idx, &stmt->where_conditions[lookup.condition_index],
                                           set_idx, set_value,
                                           lookup.uses_pk_lookup, lookup.uses_uk_lookup, rebuild_uk_needed);
        if (result > 0 && affected_rows) *affected_rows = result;
        return result >= 0;
    }
    result = execute_update_scan(tc, stmt, set_idx, set_value, rebuild_uk_needed);
    if (result > 0 && affected_rows) *affected_rows = result;
    return result >= 0;
}

void execute_update(Statement *stmt) {
    int affected_rows = 0;

    if (!execute_update_internal(stmt, &affected_rows)) return;
    if (affected_rows == 0) INFO_PRINTF("[notice] no rows matched UPDATE condition.\n");
}

int execute_update_id_fast(const char *table_name, const char *set_col, const char *set_value, const char *id_value) {
    TableCache *tc;
    int set_idx;
    int rebuild_uk_needed;
    int target_row = -1;
    char *old_record;

    if (!table_name || !set_col || !set_value || !id_value) return 0;
    tc = get_table(table_name);
    if (!tc || tc->pk_idx < 0 || tc->cache_truncated) return 0;
    if (!materialize_cached_csv_rows(tc)) return 0;
    set_idx = get_col_idx(tc, set_col);
    if (set_idx < 0) return 0;

    if (!find_pk_row(tc, id_value, &target_row)) return 1;
    old_record = slot_row(tc, target_row);
    if (!old_record) return 0;
    rebuild_uk_needed = get_uk_slot(tc, set_idx) != -1;
    return persist_updated_row(tc, target_row, old_record, set_idx, set_value,
                               rebuild_uk_needed, 1, 0) > 0;
}

static int execute_delete_internal(Statement *stmt, int *affected_rows) {
    TableCache *tc = get_table(stmt->table_name);
    MutationLookupPlan lookup;
    int result;

    if (affected_rows) *affected_rows = 0;
    if (!tc) return 0;
    if (!validate_where_columns(tc, stmt, "DELETE")) return 0;
    if (stmt->where_count == 0) {
        printf("[error] DELETE failed: WHERE condition is required.\n");
        return 0;
    }
    build_mutation_lookup_plan(tc, stmt, &lookup);
    if (tc->cache_truncated) {
        if (lookup.uses_pk_lookup && stmt->where_count == 1) {
            long pk_key;
            long tail_offset;

            if (!parse_long_value(stmt->where_conditions[lookup.condition_index].val, &pk_key)) {
                return 1;
            }
            if (find_tail_pk_offset(tc, pk_key, &tail_offset)) {
                int result = execute_delete_tail_pk_row(tc, stmt, pk_key, tail_offset);
                if (result > 0 && affected_rows) *affected_rows = result;
                return result >= 0;
            }
        }
        if (!lookup.uses_pk_lookup) {
            if (lookup.uses_uk_lookup) {
                INFO_PRINTF("[notice] DELETE on uncached table uses CSV scan fallback because tail rows have no UK offset index.\n");
            }
            if (!rewrite_truncated_delete(tc, stmt)) {
                printf("[error] DELETE failed while using CSV scan fallback.\n");
                return 0;
            }
            if (affected_rows) *affected_rows = 1;
            return 1;
        }
    }
    if (lookup.uses_pk_lookup || lookup.uses_uk_lookup) {
        result = execute_delete_single_row(tc, stmt, lookup.where_idx,
                                           &stmt->where_conditions[lookup.condition_index], lookup.uses_pk_lookup);
        if (result > 0 && affected_rows) *affected_rows = result;
        return result >= 0;
    }

    result = execute_delete_scan(tc, stmt);
    if (result > 0 && affected_rows) *affected_rows = result;
    return result >= 0;
}

void execute_delete(Statement *stmt) {
    int affected_rows = 0;

    if (!execute_delete_internal(stmt, &affected_rows)) return;
    if (affected_rows == 0) INFO_PRINTF("[notice] no rows matched DELETE condition.\n");
}

int execute_delete_id_fast(const char *table_name, const char *id_value) {
    TableCache *tc;
    int target_row = -1;
    char *old_record;

    if (!table_name || !id_value) return 0;
    tc = get_table(table_name);
    if (!tc || tc->pk_idx < 0 || tc->cache_truncated) return 0;
    if (!materialize_cached_csv_rows(tc)) return 0;

    if (!find_pk_row(tc, id_value, &target_row)) return 1;
    old_record = slot_row(tc, target_row);
    if (!old_record) return 0;
    if (!remove_record_indexes(tc, old_record)) return 0;
    if (persist_deleted_slot(tc, target_row, old_record, 0) <= 0) {
        restore_record_indexes(tc, target_row);
        return 0;
    }
    return 1;
}

int executor_execute_statement(Statement *stmt, int *matched_rows, int *affected_rows, long *generated_id) {
    if (matched_rows) *matched_rows = 0;
    if (affected_rows) *affected_rows = 0;
    if (generated_id) *generated_id = 0;
    if (!stmt) return 0;

    switch (stmt->type) {
        case STMT_INSERT:
            if (!execute_insert_internal(stmt, generated_id)) return 0;
            if (affected_rows) *affected_rows = 1;
            return 1;
        case STMT_SELECT:
            return execute_select_internal(stmt, 0, 0, matched_rows);
        case STMT_UPDATE:
            return execute_update_internal(stmt, affected_rows);
        case STMT_DELETE:
            return execute_delete_internal(stmt, affected_rows);
        default:
            return 0;
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

    if (!jungle_ensure_artifacts_absent()) return;

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
    int index_query_count = 1000;
    int uk_query_count = 1000;
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

    if (record_count <= 0) record_count = 1;
    if (record_count > MAX_RECORDS) {
        INFO_PRINTF("[notice] benchmark record count capped at MAX_RECORDS=%d.\n", MAX_RECORDS);
        record_count = MAX_RECORDS;
    }

    close_all_tables();
    open_table_count = 0;
    remove("bptree_perf_users.csv");
    remove("bptree_perf_users.delta");
    remove("bptree_perf_users.idx");

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
    if (!id_pairs || !uk_pairs) {
        printf("[error] benchmark pair arrays could not be allocated.\n");
        free(id_pairs);
        free(uk_pairs);
        return;
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
        if (tc->append_offset < 0) {
            if (fflush(tc->file) != 0 || fseek(tc->file, 0, SEEK_END) != 0) {
                printf("[error] benchmark append offset failed.\n");
                free(id_pairs);
                free(uk_pairs);
                return;
            }
            tc->append_offset = ftell(tc->file);
        }
        row_offset = tc->append_offset;
        if (!append_record_file(tc, new_line, 0)) {
            printf("[error] benchmark append failed at row %d.\n", i);
            free(id_pairs);
            free(uk_pairs);
            return;
        }
        row_id = tc->next_row_id++;
        if (!append_record_raw_memory(tc, new_line, row_id, row_offset, &inserted_slot)) {
            printf("[error] benchmark insert failed at row %d.\n", i);
            free(id_pairs);
            free(uk_pairs);
            return;
        }
        tc->records[inserted_slot] = dup_string(new_line);
        if (!tc->records[inserted_slot]) {
            printf("[error] benchmark row cache allocation failed at row %d.\n", i);
            free(id_pairs);
            free(uk_pairs);
            return;
        }
        tc->row_store[inserted_slot] = ROW_STORE_MEMORY;
        tc->row_offsets[inserted_slot] = -1;
        if (tc->row_refs) {
            tc->row_refs[inserted_slot].store = ROW_STORE_MEMORY;
            tc->row_refs[inserted_slot].offset = -1;
        }
        id_pairs[i - 1].key = i;
        id_pairs[i - 1].row_index = inserted_slot;
        uk_pairs[i - 1].key = dup_string(email);
        if (!uk_pairs[i - 1].key) {
            printf("[error] benchmark UK key allocation failed at row %d.\n", i);
            free(id_pairs);
            free(uk_pairs);
            return;
        }
        uk_pairs[i - 1].row_index = inserted_slot;
    }
    if (fflush(tc->file) != 0 || ferror(tc->file)) {
        printf("[error] benchmark insert flush failed.\n");
        free(id_pairs);
        free(uk_pairs);
        return;
    }
    tc->next_auto_id = (long)record_count + 1;
    if (!bptree_build_from_sorted(tc->id_index, id_pairs, record_count)) {
        printf("[error] benchmark PK B+ tree bulk build failed.\n");
        free(id_pairs);
        free(uk_pairs);
        return;
    }
    if (!ensure_uk_indexes(tc) || tc->uk_count != 1) {
        printf("[error] benchmark UK index is not available.\n");
        free(id_pairs);
        free(uk_pairs);
        return;
    }
    if (!bptree_string_build_from_sorted(tc->uk_indexes[0]->tree, uk_pairs, record_count)) {
        printf("[error] benchmark UK B+ tree bulk build failed.\n");
        free(id_pairs);
        for (i = 0; i < record_count; i++) free(uk_pairs[i].key);
        free(uk_pairs);
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
        snprintf(target, sizeof(target), "user%07d@test.com", ((i * 7919) % record_count) + 1);
        if (find_uk_row(tc, 1, target, &row_index)) found += row_index >= 0;
    }
    end = current_seconds();
    uk_indexed_time = end - start;

    start = current_seconds();
    for (i = 0; i < linear_query_count; i++) {
        char target[64];
        int row_index;
        snprintf(target, sizeof(target), "User%d", record_count - ((i * 7919) % record_count));
        for (row_index = 0; row_index < tc->record_count; row_index++) {
            const char *row = tc->records[row_index];
            if (row && strstr(row, target)) {
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
}

void close_all_tables(void) {
    int i;
    for (i = 0; i < open_table_count; i++) {
        TableCache *tc = &open_tables[i];

        if (tc->file && !tc->cache_truncated && tc->active_count <= MAX_RECORDS && delta_log_exists(tc)) {
            if (!compact_table_file_for_shutdown(tc)) {
                INFO_PRINTF("[warning] failed to compact delta-backed table '%s' during shutdown.\n",
                            tc->table_name);
            }
        }
        free_table_storage(tc);
    }
    open_table_count = 0;
}
