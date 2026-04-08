#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_RECORDS 25000  // 대규모 데이터 처리용
#define MAX_COLS 15

// =================================================================
// 1. 자료구조 정의
// =================================================================
typedef enum { STMT_INSERT, STMT_SELECT, STMT_UNRECOGNIZED } StatementType;
typedef enum { COL_NORMAL, COL_PK, COL_UK, COL_NN } ColumnType;

typedef struct {
    StatementType type;
    char table_name[256];
    char row_data[1024];
} Statement;

typedef struct {
    char name[50];
    ColumnType type;
} ColumnInfo;

typedef struct {
    char table_name[256];
    FILE *file;
    ColumnInfo cols[MAX_COLS];
    int col_count;
    int pk_idx, uk_idx;

    // 인덱스 (이진 탐색용)
    long pk_index[MAX_RECORDS];
    char uk_index[MAX_RECORDS][100];
    int record_count;
} TableCache;

TableCache open_tables[10];
int open_table_count = 0;

// =================================================================
// 2. 유틸리티 및 탐색 함수
// =================================================================
int compare_long(const void *a, const void *b) {
    return (*(long*)a > *(long*)b) - (*(long*)a < *(long*)b);
}

int compare_str(const void *a, const void *b) {
    return strcmp((char*)a, (char*)b);
}

void insert_pk_sorted(TableCache *cache, long val) {
    int i = cache->record_count - 1;
    while (i >= 0 && cache->pk_index[i] > val) {
        cache->pk_index[i+1] = cache->pk_index[i];
        i--;
    }
    cache->pk_index[i+1] = val;
}

void insert_uk_sorted(TableCache *cache, const char *val) {
    int i = cache->record_count - 1;
    while (i >= 0 && strcmp(cache->uk_index[i], val) > 0) {
        strcpy(cache->uk_index[i+1], cache->uk_index[i]);
        i--;
    }
    strcpy(cache->uk_index[i+1], val);
}

// =================================================================
// 3. DB 엔진: 테이블 로드 및 제약 조건 파싱
// =================================================================
TableCache* get_table(const char* name) {
    for (int i = 0; i < open_table_count; i++)
        if (strcmp(open_tables[i].table_name, name) == 0) return &open_tables[i];

    char filename[300];
    snprintf(filename, sizeof(filename), "%s.csv", name);
    FILE *f = fopen(filename, "r+");
    if (!f) { printf("[오류] 테이블 파일('%s.csv')이 존재하지 않습니다.\n", name); return NULL; }

    TableCache *tc = &open_tables[open_table_count++];
    strcpy(tc->table_name, name); tc->file = f; tc->record_count = 0;
    tc->pk_idx = -1; tc->uk_idx = -1;

    char header[1024];
    if (fgets(header, sizeof(header), f)) {
        char *token = strtok(header, ",\n\r");
        int idx = 0;
        while (token && idx < MAX_COLS) {
            strcpy(tc->cols[idx].name, token);
            if (strstr(token, "(PK)")) { tc->cols[idx].type = COL_PK; tc->pk_idx = idx; }
            else if (strstr(token, "(UK)")) { tc->cols[idx].type = COL_UK; tc->uk_idx = idx; }
            else if (strstr(token, "(NN)")) { tc->cols[idx].type = COL_NN; }
            else tc->cols[idx].type = COL_NORMAL;
            token = strtok(NULL, ",\n\r"); idx++;
        }
        tc->col_count = idx;
    }

    char line[1024];
    while (fgets(line, sizeof(line), f) && tc->record_count < MAX_RECORDS) {
        char temp[1024]; strcpy(temp, line);
        char *token = strtok(temp, ",");
        for (int i = 0; i < tc->col_count && token; i++) {
            if (i == tc->pk_idx) tc->pk_index[tc->record_count] = atol(token);
            if (i == tc->uk_idx) strcpy(tc->uk_index[tc->record_count], token);
            token = strtok(NULL, ",");
        }
        tc->record_count++;
    }
    if (tc->pk_idx != -1) qsort(tc->pk_index, tc->record_count, sizeof(long), compare_long);
    if (tc->uk_idx != -1) qsort(tc->uk_index, tc->record_count, 100, compare_str);
    return tc;
}

// =================================================================
// 4. 실행 엔진: PK/UK/NN 복합 검증 로직
// =================================================================
void execute_insert(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;

    char *vals[MAX_COLS] = {NULL};
    int val_count = 0;
    char temp[1024]; strcpy(temp, stmt->row_data);

    char *t = strtok(temp, ",");
    while (t && val_count < MAX_COLS) {
        while(isspace(*t)) t++;
        vals[val_count++] = t;
        t = strtok(NULL, ",");
    }

    // [핵심 로직] 제약 조건 검사
    for (int i = 0; i < tc->col_count; i++) {
        char *val = (i < val_count) ? vals[i] : NULL;

        // 1. 필수 값 체크 (PK, UK, NN 모두 해당)
        if (tc->cols[i].type != COL_NORMAL && (val == NULL || strlen(val) == 0)) {
            printf("[실패] '%s' 컬럼은 필수 입력(NOT NULL) 항목입니다.\n", tc->cols[i].name);
            return;
        }

        // 2. 중복 체크 (PK, UK만 이진 탐색)
        if (i == tc->pk_idx && val) {
            long v = atol(val);
            if (bsearch(&v, tc->pk_index, tc->record_count, sizeof(long), compare_long)) {
                printf("[실패] PK 중복 발생: %ld\n", v); return;
            }
        }
        if (i == tc->uk_idx && val) {
            if (bsearch(val, tc->uk_index, tc->record_count, 100, compare_str)) {
                printf("[실패] UK 중복 발생: '%s'\n", val); return;
            }
        }
    }

    // 데이터 기록
    fseek(tc->file, 0, SEEK_END);
    for (int i = 0; i < tc->col_count; i++) {
        fprintf(tc->file, "%s", (i < val_count && vals[i]) ? vals[i] : "NULL");
        if (i < tc->col_count - 1) fprintf(tc->file, ",");
    }
    fprintf(tc->file, "\n"); fflush(tc->file);

    // 인덱스 갱신
    if (tc->pk_idx != -1) insert_pk_sorted(tc, atol(vals[tc->pk_idx]));
    if (tc->uk_idx != -1) insert_uk_sorted(tc, vals[tc->uk_idx]);
    tc->record_count++;
    printf("[성공] 데이터 추가 완료 (총 %d건)\n", tc->record_count);
}

void execute_select(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;
    fseek(tc->file, 0, SEEK_SET);
    printf("\n=== [%s] Result ===\n", tc->table_name);
    char line[1024];
    while (fgets(line, sizeof(line), tc->file)) printf("%s", line);
    printf("====================\n");
    clearerr(tc->file);
}

// =================================================================
// 5. 파서 및 메인
// =================================================================
int parse_statement(const char *sql, Statement *stmt) {
    char cmd[20], table[256], vals[1024], garbage[256];
    if (sscanf(sql, "%19s", cmd) != 1) return 0;
    for(int i=0; cmd[i]; i++) cmd[i] = toupper(cmd[i]);

    if (strcmp(cmd, "INSERT") == 0) {
        if (sscanf(sql, "INSERT INTO %255s VALUES (%1023[^)]) %255s", table, vals, garbage) == 2) {
            stmt->type = STMT_INSERT; strcpy(stmt->table_name, table); strcpy(stmt->row_data, vals);
            return 1;
        }
    } else if (strcmp(cmd, "SELECT") == 0) {
        if (sscanf(sql, "SELECT * FROM %255s %255s", table, garbage) == 1) {
            stmt->type = STMT_SELECT; strcpy(stmt->table_name, table);
            return 1;
        }
    }
    return 0;
}

int main(int argc, char *argv[]) {
    char filename[256];
    if (argc >= 2) strcpy(filename, argv[1]);
    else { printf("SQL 파일명 입력: "); scanf("%255s", filename); }

    FILE *f = fopen(filename, "r");
    if (!f) return 1;

    char buf[4096]; int idx = 0, q = 0, ch;
    while ((ch = fgetc(f)) != EOF) {
        if (ch == '\'') q = !q;
        if (ch == ';' && !q) {
            buf[idx] = '\0';
            char *s = buf; while (isspace(*s)) s++;
            if (strlen(s) > 0) {
                Statement stmt;
                if (parse_statement(s, &stmt)) {
                    if (stmt.type == STMT_INSERT) execute_insert(&stmt);
                    else execute_select(&stmt);
                } else printf("[에러] 문법 오류: %s\n", s);
            }
            idx = 0;
        } else if (idx < 4095) buf[idx++] = (char)ch;
    }
    fclose(f);
    for(int i=0; i<open_table_count; i++) fclose(open_tables[i].file);
    return 0;
}