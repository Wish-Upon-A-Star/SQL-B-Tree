#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_RECORDS 25000
#define MAX_COLS 15
#define MAX_TABLES 10
#define MAX_UKS 5

// [1] 자료구조
typedef enum { STMT_INSERT, STMT_SELECT, STMT_DELETE, STMT_UPDATE, STMT_UNRECOGNIZED } StatementType;
typedef enum { COL_NORMAL, COL_PK, COL_UK, COL_NN } ColumnType;

typedef struct {
    StatementType type;
    char table_name[256];
    char select_cols[256];
    char row_data[1024];
    char set_col[50], set_val[256];
    char where_col[50], where_val[256];
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
    int pk_idx, uk_indices[MAX_UKS], uk_count;
    long pk_index[MAX_RECORDS];
    char records[MAX_RECORDS][1024]; 
    int record_count;
} TableCache;

TableCache open_tables[MAX_TABLES];
int open_table_count = 0;

// [2] 유틸리티 함수
int compare_long(const void *a, const void *b) { return (*(long*)a > *(long*)b) - (*(long*)a < *(long*)b); }

int find_in_pk_index(TableCache *tc, long val) {
    if (tc->record_count == 0) return -1;
    long *found = bsearch(&val, tc->pk_index, tc->record_count, sizeof(long), compare_long);
    return found ? (int)(found - tc->pk_index) : -1;
}

void parse_csv_row(char *row, char *fields[MAX_COLS]) {
    int i = 0; char *p = row; fields[i++] = p;
    while (*p && i < MAX_COLS) {
        if (*p == ',') { *p = '\0'; fields[i++] = p + 1; }
        p++;
    }
}

int get_col_idx(TableCache *tc, const char *col_name) {
    if (!col_name || strlen(col_name) == 0) return -1; // 빈 이름 방지
    for (int i = 0; i < tc->col_count; i++) {
        if (strstr(tc->cols[i].name, col_name)) return i;
    }
    return -1;
}

void rewrite_file(TableCache *tc) {
    fclose(tc->file);
    char filename[300]; snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    tc->file = fopen(filename, "w+");
    for (int i = 0; i < tc->col_count; i++) fprintf(tc->file, "%s%s", tc->cols[i].name, (i == tc->col_count - 1 ? "\n" : ","));
    for (int i = 0; i < tc->record_count; i++) fprintf(tc->file, "%s\n", tc->records[i]);
    fflush(tc->file);
}

void insert_pk_sorted(TableCache *tc, long val, const char* row_str) {
    int i = tc->record_count - 1;
    while (i >= 0 && tc->pk_index[i] > val) {
        tc->pk_index[i+1] = tc->pk_index[i];
        strcpy(tc->records[i+1], tc->records[i]);
        i--;
    }
    tc->pk_index[i+1] = val;
    strcpy(tc->records[i+1], row_str);
}

// [3] 실행 엔진
TableCache* get_table(const char* name) {
    for (int i = 0; i < open_table_count; i++) if (strcmp(open_tables[i].table_name, name) == 0) return &open_tables[i];
    char filename[300]; snprintf(filename, sizeof(filename), "%s.csv", name);
    FILE *f = fopen(filename, "r+"); if (!f) return NULL;
    TableCache *tc = &open_tables[open_table_count++];
    strcpy(tc->table_name, name); tc->file = f; tc->record_count = 0; tc->pk_idx = -1;
    char header[1024];
    if (fgets(header, sizeof(header), f)) {
        char *token = strtok(header, ",\n\r"); int idx = 0;
        while (token && idx < MAX_COLS) {
            strcpy(tc->cols[idx].name, token);
            if (strstr(token, "(PK)")) { tc->cols[idx].type = COL_PK; tc->pk_idx = idx; }
            token = strtok(NULL, ",\n\r"); idx++;
        }
        tc->col_count = idx;
    }
    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        char temp[1024]; strcpy(temp, line); char *token = strtok(temp, ",");
        long cp = 0; for(int i=0; i<tc->col_count && token; i++) { if(i==tc->pk_idx) cp = atol(token); token = strtok(NULL, ","); }
        char *nl = strpbrk(line, "\r\n"); if(nl) *nl = '\0';
        insert_pk_sorted(tc, cp, line); tc->record_count++;
    }
    return tc;
}

void execute_insert(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name); if (!tc) return;
    char *vals[MAX_COLS]; int val_count = 0; char temp[1024]; strcpy(temp, stmt->row_data);
    char *t = strtok(temp, ",");
    while (t && val_count < MAX_COLS) { while(isspace(*t)) t++; vals[val_count++] = t; t = strtok(NULL, ","); }
    long new_id = atol(vals[tc->pk_idx]);
    if (find_in_pk_index(tc, new_id) != -1) { printf("[실패] PK 중복: %ld\n", new_id); return; }
    char new_line[1024] = "";
    for (int i = 0; i < tc->col_count; i++) {
        strcat(new_line, (i < val_count) ? vals[i] : "NULL");
        if (i < tc->col_count - 1) strcat(new_line, ",");
    }
    insert_pk_sorted(tc, new_id, new_line); tc->record_count++;
    rewrite_file(tc); printf("[성공] 데이터 추가 완료 (ID: %ld)\n", new_id);
}

void execute_select(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name); if (!tc) return;
    int where_idx = get_col_idx(tc, stmt->where_col);
    printf("\n--- [%s] 조회 결과 --- (조건: %s)\n", tc->table_name, where_idx != -1 ? stmt->where_val : "전체");
    for (int i = 0; i < tc->record_count; i++) {
        if (where_idx != -1) {
            char temp[1024]; strcpy(temp, tc->records[i]); char *fields[MAX_COLS]; parse_csv_row(temp, fields);
            if (fields[where_idx] && strcmp(fields[where_idx], stmt->where_val) == 0) printf("%s\n", tc->records[i]);
        } else printf("%s\n", tc->records[i]);
    }
}

void execute_update(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name); if (!tc) return;
    int where_idx = get_col_idx(tc, stmt->where_col);
    int set_idx = get_col_idx(tc, stmt->set_col);
    if (where_idx == -1 || set_idx == -1) return;
    int count = 0;
    for (int i = 0; i < tc->record_count; i++) {
        char temp[1024]; strcpy(temp, tc->records[i]); char *fields[MAX_COLS]; parse_csv_row(temp, fields);
        if (fields[where_idx] && strcmp(fields[where_idx], stmt->where_val) == 0) {
            char new_row[1024] = "";
            for (int j = 0; j < tc->col_count; j++) {
                strcat(new_row, (j == set_idx) ? stmt->set_val : fields[j]);
                if (j < tc->col_count - 1) strcat(new_row, ",");
            }
            strcpy(tc->records[i], new_row); count++;
        }
    }
    if (count > 0) { rewrite_file(tc); printf("[성공] %d개의 행이 수정되었습니다.\n", count); }
}

void execute_delete(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name); if (!tc) return;
    int idx = find_in_pk_index(tc, atol(stmt->where_val));
    if (idx != -1) {
        for (int i = idx; i < tc->record_count - 1; i++) { tc->pk_index[i] = tc->pk_index[i+1]; strcpy(tc->records[i], tc->records[i+1]); }
        tc->record_count--; rewrite_file(tc); printf("[성공] ID %s 삭제 완료\n", stmt->where_val);
    }
}

// [4] 파서 및 메인
int parse_statement(const char *sql, Statement *stmt) {
    memset(stmt, 0, sizeof(Statement));
    if (strstr(sql, "SELECT")) {
        if (sscanf(sql, "SELECT * FROM %255s WHERE %49s = %255s", stmt->table_name, stmt->where_col, stmt->where_val) < 1) return 0;
        stmt->type = STMT_SELECT; return 1;
    } else if (strstr(sql, "INSERT")) {
        if (sscanf(sql, "INSERT INTO %255s VALUES (%1023[^)])", stmt->table_name, stmt->row_data) < 2) return 0;
        stmt->type = STMT_INSERT; return 1;
    } else if (strstr(sql, "UPDATE")) {
        if (sscanf(sql, "UPDATE %255s SET %49s = %255s WHERE %49s = %255s", stmt->table_name, stmt->set_col, stmt->set_val, stmt->where_col, stmt->where_val) < 5) return 0;
        stmt->type = STMT_UPDATE; return 1;
    } else if (strstr(sql, "DELETE")) {
        if (sscanf(sql, "DELETE FROM %255s WHERE %49s = %255s", stmt->table_name, stmt->where_col, stmt->where_val) < 3) return 0;
        stmt->type = STMT_DELETE; return 1;
    }
    return 0;
}

int main(int argc, char *argv[]) {
    char filename[256]; if (argc >= 2) strcpy(filename, argv[1]); else return 1;
    FILE *f = fopen(filename, "r"); if (!f) return 1;
    char buf[4096]; int idx = 0, q = 0, ch;
    while ((ch = fgetc(f)) != EOF) {
        if (ch == '-' && !q) { int n = fgetc(f); if (n == '-') { while ((ch = fgetc(f)) != EOF && ch != '\n'); continue; } ungetc(n, f); }
        if (ch == '\'') q = !q;
        if (ch == ';' && !q) {
            buf[idx] = '\0'; char *s = buf; while (isspace(*s)) s++;
            if (strlen(s) > 0) { Statement stmt; if (parse_statement(s, &stmt)) {
                if (stmt.type == STMT_INSERT) execute_insert(&stmt);
                else if (stmt.type == STMT_SELECT) execute_select(&stmt);
                else if (stmt.type == STMT_DELETE) execute_delete(&stmt);
                else if (stmt.type == STMT_UPDATE) execute_update(&stmt);
            } }
            idx = 0;
        } else if (idx < 4095) buf[idx++] = (char)ch;
    }
    fclose(f); return 0;
}