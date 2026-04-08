#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "executor.h"

TableCache open_tables[MAX_TABLES];
int open_table_count = 0;
static unsigned long long g_table_access_seq = 0;

static void insert_pk_sorted(TableCache *tc, long val, const char* row_str);

/* qsort/bsearch에서 long 비교에 쓰이는 비교 함수입니다. */
static int compare_long(const void *a, const void *b) {
    long val_a = *(long *)a;
    long val_b = *(long *)b;
    return (val_a > val_b) - (val_a < val_b);
}

/* PK 인덱스 배열에서 주어진 PK 값을 이분 탐색으로 찾습니다. */
static int find_in_pk_index(TableCache *tc, long val) {
    if (tc->record_count == 0 || tc->pk_idx == -1) return -1;
    long *found = bsearch(&val, tc->pk_index, tc->record_count, sizeof(long), compare_long);
    return found ? (int)(found - tc->pk_index) : -1;
}

/* 문자열 앞뒤 공백을 제거하고, 필요하면 양끝의 홑따옴표를 제거합니다. */
void trim_and_unquote(char *str) {
    if (!str) return;
    char *start = str;
    while (isspace((unsigned char)*start)) start++;
    char *end = start + strlen(start) - 1;
    while (end > start && isspace((unsigned char)*end)) end--;
    *(end + 1) = '\0';
    if (start[0] == '\'' && end > start && *end == '\'') {
        start++;
        *end = '\0';
    }
    if (start != str) {
        memmove(str, start, strlen(start) + 1);
    }
}

/* 값 비교를 위해 둘 다 trim/quote 정규화한 뒤 동일 여부를 반환합니다. */
int compare_value(const char *field, const char *search_val) {
    char f_buf[256];
    char v_buf[256];
    strncpy(f_buf, field ? field : "", 255);
    f_buf[255] = '\0';
    strncpy(v_buf, search_val ? search_val : "", 255);
    v_buf[255] = '\0';
    trim_and_unquote(f_buf);
    trim_and_unquote(v_buf);
    return strcmp(f_buf, v_buf) == 0;
}

/* CSV 한 줄을 쉼표 기준으로 나누어 fields 배열에 포인터를 채웁니다. */
void parse_csv_row(const char *row, char *fields[MAX_COLS], char *buffer) {
    strncpy(buffer, row, 1023);
    buffer[1023] = '\0';
    int i = 0;
    int in_quotes = 0;
    char *p = buffer;
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

/* 컬럼 이름으로 컬럼 인덱스를 반환합니다. 못 찾으면 -1. */
static int get_col_idx(TableCache *tc, const char *col_name) {
    if (!col_name || strlen(col_name) == 0) return -1;
    int i;
    for (i = 0; i < tc->col_count; i++) {
        if (strcmp(tc->cols[i].name, col_name) == 0) return i;
    }
    return -1;
}

/* 메모리 캐시 상태를 현재까지의 CSV 파일 형태로 다시 저장합니다. */
void rewrite_file(TableCache *tc) {
    int i;
    if (tc->file) fclose(tc->file);
    char filename[300];
    snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    tc->file = fopen(filename, "w+");
    if (!tc->file) return;
    for (i = 0; i < tc->col_count; i++) {
        fprintf(tc->file, "%s", tc->cols[i].name);
        if (tc->cols[i].type == COL_PK) {
            fprintf(tc->file, "(PK)");
        } else if (tc->cols[i].type == COL_UK) {
            fprintf(tc->file, "(UK)");
        } else if (tc->cols[i].type == COL_NN) {
            fprintf(tc->file, "(NN)");
        }
        fprintf(tc->file, "%s", (i == tc->col_count - 1 ? "\n" : ","));
    }
    for (i = 0; i < tc->record_count; i++) {
        fprintf(tc->file, "%s\n", tc->records[i]);
    }
    fflush(tc->file);
}

/* 접근 이벤트를 남겨 LRU 교체 순서를 갱신합니다. */
static void touch_table(TableCache *tc) {
    tc->last_used_seq = ++g_table_access_seq;
}

/* 캐시 슬롯을 완전히 비우고 기본값으로 재설정합니다. */
static void reset_table_cache(TableCache *tc) {
    memset(tc, 0, sizeof(TableCache));
    tc->file = NULL;
    tc->pk_idx = -1;
}

/* 현재 가장 오래된 접근 순서의 슬롯을 찾아 반환합니다. */
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

/* 파일을 열어 헤더와 레코드를 TableCache에 적재합니다. */
static int load_table_contents(TableCache *tc, const char *name, FILE *f) {
    reset_table_cache(tc);
    strncpy(tc->table_name, name, 255);
    tc->table_name[255] = '\0';
    tc->file = f;
    tc->record_count = 0;
    tc->pk_idx = -1;
    tc->col_count = 0;
    tc->uk_count = 0;
    tc->last_used_seq = ++g_table_access_seq;

    char header[1024];
    if (fgets(header, sizeof(header), f)) {
        char *token = strtok(header, ",\r\n");
        int idx = 0;
        while (token && idx < MAX_COLS) {
            char *paren = strchr(token, '(');
            if (paren) {
                int len = (int)(paren - token);
                if (len >= (int)sizeof(tc->cols[idx].name)) len = (int)sizeof(tc->cols[idx].name) - 1;
                strncpy(tc->cols[idx].name, token, len);
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

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        if (tc->record_count >= MAX_RECORDS) {
            printf("[오류] 레코드 개수 초과: '%s'의 최대 레코드 수(%d)에 도달해 일부 데이터를 건너뜁니다.\n", name, MAX_RECORDS);
            continue;
        }

        char buffer[1024];
        char *fields[MAX_COLS] = {0};
        parse_csv_row(line, fields, buffer);
        long cp = (tc->pk_idx != -1 && fields[tc->pk_idx]) ? atol(fields[tc->pk_idx]) : 0;
        char *nl = strpbrk(line, "\r\n");
        if (nl) *nl = '\0';
        insert_pk_sorted(tc, cp, line);
        tc->record_count++;
    }
    return 1;
}

/* PK 정렬 배열을 유지하며 새 레코드를 적절한 위치에 삽입합니다. */
void insert_pk_sorted(TableCache *tc, long val, const char* row_str) {
    if (tc->record_count >= MAX_RECORDS) {
        return;
    }

    if (tc->pk_idx == -1) {
        strncpy(tc->records[tc->record_count], row_str, 1023);
        tc->records[tc->record_count][1023] = '\0';
        return;
    }
    int i = tc->record_count - 1;
    while (i >= 0 && tc->pk_index[i] > val) {
        tc->pk_index[i + 1] = tc->pk_index[i];
        strncpy(tc->records[i + 1], tc->records[i], 1023);
        tc->records[i + 1][1023] = '\0';
        i--;
    }
    tc->pk_index[i + 1] = val;
    strncpy(tc->records[i + 1], row_str, 1023);
    tc->records[i + 1][1023] = '\0';
}

/*
 * 캐시에 이미 열려 있으면 재사용하고,
 * 없으면 파일을 열어 로드합니다.
 * 캐시가 꽉 차면 LRU 정책으로 하나를 닫고 교체합니다.
 */
TableCache* get_table(const char* name) {
    int i;
    for (i = 0; i < open_table_count; i++) {
        if (strcmp(open_tables[i].table_name, name) == 0) {
            touch_table(&open_tables[i]);
            return &open_tables[i];
        }
    }

    char filename[300];
    snprintf(filename, sizeof(filename), "%s.csv", name);
    FILE *f = fopen(filename, "r+");
    if (!f) {
        printf("[알림] '%s.csv' 파일을 열 수 없습니다.\n", name);
        return NULL;
    }

    TableCache *tc = NULL;
    if (open_table_count < MAX_TABLES) {
        tc = &open_tables[open_table_count++];
    } else {
        int evict_idx = find_lru_table_index();
        tc = &open_tables[evict_idx];
        printf("[안내] LRU 정책으로 '%s'를 닫고 '%s'를 새로 엽니다.\n", tc->table_name, name);
        if (tc->file) fclose(tc->file);
    }

    if (!load_table_contents(tc, name, f)) {
        if (open_table_count < MAX_TABLES) open_table_count--;
        fclose(f);
        return NULL;
    }
    return tc;
}

/* INSERT: NN/PK/UK 제약을 검사한 뒤 레코드를 추가하고 파일을 갱신합니다. */
void execute_insert(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;

    char buffer[1024];
    char *vals[MAX_COLS] = {0};
    parse_csv_row(stmt->row_data, vals, buffer);

    int val_count = 0;
    while (vals[val_count] && val_count < MAX_COLS) val_count++;

    long new_id = 0;
    int i;
    int r;
    char *endptr = NULL;

    if (tc->record_count >= MAX_RECORDS) {
        printf("[오류] INSERT 실패: 레코드 개수 초과(최대 %d건).\n", MAX_RECORDS);
        return;
    }
    for (i = 0; i < tc->col_count; i++) {
        char *val = (i < val_count && vals[i]) ? vals[i] : "";
        char normalized_val[256];
        strncpy(normalized_val, val, sizeof(normalized_val) - 1);
        normalized_val[sizeof(normalized_val) - 1] = '\0';
        trim_and_unquote(normalized_val);

        if (tc->cols[i].type == COL_NN && strlen(normalized_val) == 0) {
            printf("[오류] INSERT 실패: '%s' 컬럼은 NN 제약을 위반했습니다.\n", tc->cols[i].name);
            return;
        }

        if (i == tc->pk_idx) {
            if (strlen(normalized_val) == 0) {
                printf("[오류] INSERT 실패: PK 컬럼 '%s' 값이 비어 있습니다.\n", tc->cols[i].name);
                return;
            }
            new_id = strtol(normalized_val, &endptr, 10);
            if (endptr == normalized_val || *endptr != '\0') {
                printf("[오류] INSERT 실패: PK 컬럼 '%s'은(는) 정수값이어야 합니다.\n", tc->cols[i].name);
                return;
            }
            if (find_in_pk_index(tc, new_id) != -1) {
                printf("[오류] INSERT 실패: PK 중복 값(%ld)이 이미 존재합니다.\n", new_id);
                return;
            }
        }

        if (tc->cols[i].type == COL_UK && strlen(normalized_val) > 0) {
            for (r = 0; r < tc->record_count; r++) {
                char row_buf[1024];
                char *f[MAX_COLS] = {0};
                parse_csv_row(tc->records[r], f, row_buf);
                if (compare_value(f[i], normalized_val)) {
                    printf("[오류] INSERT 실패: '%s'는 UK 제약을 위반합니다.\n", normalized_val);
                    return;
                }
            }
        }
    }

    char new_line[1024] = "";
    size_t offset = 0;
    for (i = 0; i < tc->col_count; i++) {
        const char *v = (i < val_count && vals[i]) ? vals[i] : ((tc->cols[i].type == COL_NN) ? "" : "NULL");
        char normalized_storage_val[1024];
        char formatted_val[1024];
        strncpy(normalized_storage_val, v, sizeof(normalized_storage_val) - 1);
        normalized_storage_val[sizeof(normalized_storage_val) - 1] = '\0';
        trim_and_unquote(normalized_storage_val);
        if (strchr(normalized_storage_val, ',')) {
            snprintf(formatted_val, sizeof(formatted_val), "'%.*s'", (int)(sizeof(formatted_val) - 3), normalized_storage_val);
            v = formatted_val;
        } else {
            v = normalized_storage_val;
        }
        int w = snprintf(new_line + offset, sizeof(new_line) - offset, "%s%s", v, (i < tc->col_count - 1) ? "," : "");
        if (w < 0 || (size_t)w >= sizeof(new_line) - offset) break;
        offset += w;
    }

    insert_pk_sorted(tc, new_id, new_line);
    tc->record_count++;
    rewrite_file(tc);
    printf("[완료] INSERT 처리했습니다.\n");
}

/* SELECT: where 조건이 있으면 조건 행만, 없으면 전부 출력합니다. */
void execute_select(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;

    int where_idx = get_col_idx(tc, stmt->where_col);
    printf("\n--- [SELECT RESULT] table=%s ---\n", tc->table_name);
    int i;
    for (i = 0; i < tc->record_count; i++) {
        if (where_idx != -1) {
            char row_buf[1024];
            char *fields[MAX_COLS] = {0};
            parse_csv_row(tc->records[i], fields, row_buf);
            if (compare_value(fields[where_idx], stmt->where_val)) {
                printf("%s\n", tc->records[i]);
            }
        } else {
            printf("%s\n", tc->records[i]);
        }
    }
}

/* UPDATE: where 조건 대상 행을 찾아 set 컬럼을 변경합니다. */
void execute_update(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;

    int where_idx = get_col_idx(tc, stmt->where_col);
    int set_idx = get_col_idx(tc, stmt->set_col);
    if (where_idx == -1 || set_idx == -1) {
        printf("[오류] WHERE 조건을 해석할 수 없습니다.\n");
        return;
    }

    if (set_idx == tc->pk_idx) {
        printf("[오류] PK(기본키) 컬럼은 UPDATE에서 변경할 수 없습니다.\n");
        return;
    }

    trim_and_unquote(stmt->set_val);
    if (tc->cols[set_idx].type == COL_NN && strlen(stmt->set_val) == 0) {
        printf("[오류] UPDATE 실패: '%s'는 NN 제약을 위반했습니다.\n", tc->cols[set_idx].name);
        return;
    }

    int match_flags[MAX_RECORDS] = {0};
    int target_count = 0;
    int i;
    int r;
    int j;
    for (i = 0; i < tc->record_count; i++) {
        char row_buf[1024];
        char *f[MAX_COLS] = {0};
        parse_csv_row(tc->records[i], f, row_buf);
        if (compare_value(f[where_idx], stmt->where_val)) {
            match_flags[i] = 1;
            target_count++;
        }
    }

    if (tc->cols[set_idx].type == COL_UK) {
        if (target_count > 1) {
            printf("[오류] UPDATE 실패: WHERE 조건이 여러 행을 가리켜 UK 제약이 깨집니다.\n");
            return;
        }
        for (r = 0; r < tc->record_count; r++) {
            if (match_flags[r]) continue;
            char row_buf[1024];
            char *f[MAX_COLS] = {0};
            parse_csv_row(tc->records[r], f, row_buf);
            if (compare_value(f[set_idx], stmt->set_val)) {
                printf("[오류] UPDATE 실패: '%s'는 UK 제약 위반 값입니다.\n", stmt->set_val);
                return;
            }
        }
    }

    int count = 0;
    for (i = 0; i < tc->record_count; i++) {
        if (!match_flags[i]) continue;
        char row_buf[1024];
        char *fields[MAX_COLS] = {0};
        parse_csv_row(tc->records[i], fields, row_buf);

        char new_row[1024] = "";
        size_t offset = 0;
        for (j = 0; j < tc->col_count; j++) {
            const char *val = (j == set_idx) ? stmt->set_val : (fields[j] ? fields[j] : "");
            int w = snprintf(new_row + offset, sizeof(new_row) - offset, "%s%s", val, (j < tc->col_count - 1) ? "," : "");
            if (w < 0 || (size_t)w >= sizeof(new_row) - offset) break;
            offset += w;
        }
        strncpy(tc->records[i], new_row, 1023);
        tc->records[i][1023] = '\0';
        count++;
    }

    if (count > 0) {
        rewrite_file(tc);
        printf("[완료] %d개의 행이 수정되었습니다.\n", count);
    } else {
        printf("[알림] 조건에 맞는 행이 없습니다.\n");
    }
}

/* DELETE: where 조건을 만족하는 행을 삭제합니다. */
void execute_delete(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;

    int where_idx = get_col_idx(tc, stmt->where_col);
    if (where_idx == -1) {
        printf("[오류] WHERE 조건을 해석할 수 없습니다.\n");
        return;
    }

    int count = 0;
    int i;
    int j;
    for (i = 0; i < tc->record_count; i++) {
        char row_buf[1024];
        char *fields[MAX_COLS] = {0};
        parse_csv_row(tc->records[i], fields, row_buf);
        if (compare_value(fields[where_idx], stmt->where_val)) {
            for (j = i; j < tc->record_count - 1; j++) {
                tc->pk_index[j] = tc->pk_index[j + 1];
                strncpy(tc->records[j], tc->records[j + 1], 1023);
                tc->records[j][1023] = '\0';
            }
            tc->record_count--;
            count++;
            i--;
        }
    }

    if (count > 0) {
        rewrite_file(tc);
        printf("[완료] %d개의 행이 삭제되었습니다.\n", count);
    } else {
        printf("[알림] 삭제할 행이 없습니다.\n");
    }
}

/* 프로그램 종료 시 열려 있는 테이블 파일 핸들을 닫습니다. */
void close_all_tables(void) {
    int i;
    for (i = 0; i < open_table_count; i++) {
        if (open_tables[i].file) fclose(open_tables[i].file);
    }
}

