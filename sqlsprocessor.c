#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_RECORDS 25000
#define MAX_COLS 15
#define MAX_TABLES 10
#define MAX_UKS 5  // 누락되었던 상수 추가

// [1] 자료구조 정의
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

// [2] 보조 함수 (이진 탐색, 정렬, 파일 관리)

int compare_long(const void *a, const void *b) { 
    return (*(long*)a > *(long*)b) - (*(long*)a < *(long*)b); 
}

int find_in_pk_index(TableCache *tc, long val) {
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

void rewrite_file(TableCache *tc) {
    fclose(tc->file);
    char filename[300]; snprintf(filename, sizeof(filename), "%s.csv", tc->table_name);
    tc->file = fopen(filename, "w+");
    for (int i = 0; i < tc->col_count; i++) {
        fprintf(tc->file, "%s%s", tc->cols[i].name, (i == tc->col_count - 1 ? "\n" : ","));
    }
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

TableCache* get_table(const char* name) {
    for (int i = 0; i < open_table_count; i++)
        if (strcmp(open_tables[i].table_name, name) == 0) return &open_tables[i];
    if (open_table_count >= MAX_TABLES) return NULL;

    char filename[300]; snprintf(filename, sizeof(filename), "%s.csv", name);
    FILE *f = fopen(filename, "r+");
    if (!f) return NULL;

    TableCache *tc = &open_tables[open_table_count++];
    strcpy(tc->table_name, name); tc->file = f; 
    tc->record_count = 0; tc->pk_idx = -1; tc->uk_count = 0;

    char header[1024];
    if (fgets(header, sizeof(header), f)) {
        char *token = strtok(header, ",\n\r");
        int idx = 0;
        while (token && idx < MAX_COLS) {
            strcpy(tc->cols[idx].name, token);
            if (strstr(token, "(PK)")) { tc->cols[idx].type = COL_PK; tc->pk_idx = idx; }
            else if (strstr(token, "(UK)")) { 
                tc->cols[idx].type = COL_UK; 
                if (tc->uk_count < MAX_UKS) tc->uk_indices[tc->uk_count++] = idx;
            }
            else if (strstr(token, "(NN)")) tc->cols[idx].type = COL_NN;
            else tc->cols[idx].type = COL_NORMAL;
            token = strtok(NULL, ",\n\r"); idx++;
        }
        tc->col_count = idx;
    }
    char line[1024];
    while (fgets(line, sizeof(line), f) && tc->record_count < MAX_RECORDS) {
        char temp[1024]; strcpy(temp, line);
        char *token = strtok(temp, ",\n\r");
        long current_pk = 0;
        for (int i = 0; i < tc->col_count && token; i++) {
            if (i == tc->pk_idx) current_pk = atol(token);
            token = strtok(NULL, ",\n\r");
        }
        char *nl = strpbrk(line, "\r\n"); if(nl) *nl = '\0';
        insert_pk_sorted(tc, current_pk, line);
        tc->record_count++;
    }
    return tc;
}

int get_col_idx(TableCache *tc, const char *col_name) {
    for (int i = 0; i < tc->col_count; i++) {
        if (strstr(tc->cols[i].name, col_name)) return i;
    }
    return -1;
}

// [3] 실행 엔진 (개선된 범용 버전)

void execute_insert(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;
    char *vals[MAX_COLS] = {NULL}; int val_count = 0;
    char temp[1024]; strcpy(temp, stmt->row_data);
    char *t = strtok(temp, ",");
    while (t && val_count < MAX_COLS) {
        while(isspace(*t)) t++;
        vals[val_count++] = t; t = strtok(NULL, ",");
    }
    long new_id = atol(vals[tc->pk_idx]);
    if (find_in_pk_index(tc, new_id) != -1) {
        printf("[실패] PK 중복 발생: %ld\n", new_id); return;
    }
    for (int i = 0; i < tc->col_count; i++) {
        if (tc->cols[i].type == COL_NN && (i >= val_count || strlen(vals[i]) == 0)) {
            printf("[실패] %s(NN) 컬럼은 비워둘 수 없습니다.\n", tc->cols[i].name); return;
        }
    }
    char new_line[1024] = "";
    for (int i = 0; i < tc->col_count; i++) {
        strcat(new_line, (i < val_count && vals[i]) ? vals[i] : "NULL");
        if (i < tc->col_count - 1) strcat(new_line, ",");
    }
    insert_pk_sorted(tc, new_id, new_line); tc->record_count++;
    rewrite_file(tc); printf("[성공] 데이터 추가 완료 (ID: %ld)\n", new_id);
}

void execute_select(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;
    int where_idx = get_col_idx(tc, stmt->where_col);
    printf("\n--- [%s] 조회 결과 --- (조건: %s)\n", tc->table_name, strlen(stmt->where_col) > 0 ? stmt->where_val : "전체");
    for (int i = 0; i < tc->record_count; i++) {
        if (where_idx != -1) {
            char temp[1024]; strcpy(temp, tc->records[i]);
            char *fields[MAX_COLS] = {NULL}; parse_csv_row(temp, fields);
            if (fields[where_idx] && strcmp(fields[where_idx], stmt->where_val) == 0) printf("%s\n", tc->records[i]);
        } else printf("%s\n", tc->records[i]);
    }
}

void execute_delete(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc || tc->pk_idx == -1) return;
    int idx = find_in_pk_index(tc, atol(stmt->where_val));
    if (idx != -1) {
        for (int i = idx; i < tc->record_count - 1; i++) {
            tc->pk_index[i] = tc->pk_index[i+1]; strcpy(tc->records[i], tc->records[i+1]);
        }
        tc->record_count--; rewrite_file(tc); printf("[성공] ID %s 삭제 완료\n", stmt->where_val);
    } else printf("[실패] 대상을 찾을 수 없습니다.\n");
}

void execute_update(Statement *stmt) {
    TableCache *tc = get_table(stmt->table_name);
    if (!tc) return;
    int set_idx = get_col_idx(tc, stmt->set_col);
    int where_idx = get_col_idx(tc, stmt->where_col);
    int updated = 0;
    for (int i = 0; i < tc->record_count; i++) {
        char temp[1024]; strcpy(temp, tc->records[i]);
        char *fields[MAX_COLS] = {NULL}; parse_csv_row(temp, fields);
        if (fields[where_idx] && strcmp(fields[where_idx], stmt->where_val) == 0) {
            char new_row[1024] = "";
            for (int j = 0; j < tc->col_count; j++) {
                strcat(new_row, (j == set_idx) ? stmt->set_val : fields[j]);
                if (j < tc->col_count - 1) strcat(new_row, ",");
            }
            strcpy(tc->records[i], new_row); updated++;
        }
    }
    if (updated) { rewrite_file(tc); printf("[성공] %d개의 행이 수정되었습니다.\n", updated); }
}

int parse_statement(const char *sql, Statement *stmt) {
    char cmd[20]; memset(stmt, 0, sizeof(Statement));
    if (sscanf(sql, "%19s", cmd) != 1) return 0;
    for(int i=0; cmd[i]; i++) cmd[i] = toupper(cmd[i]);
    if (strcmp(cmd, "SELECT") == 0) {
        if (sscanf(sql, "SELECT %255s FROM %255s WHERE %49s = %255s", stmt->select_cols, stmt->table_name, stmt->where_col, stmt->where_val) >= 2) {
            stmt->type = STMT_SELECT; return 1;
        }
    } else if (strcmp(cmd, "DELETE") == 0) {
        if (sscanf(sql, "DELETE FROM %255s WHERE %49s = %255s", stmt->table_name, stmt->where_col, stmt->where_val) >= 2) {
            stmt->type = STMT_DELETE; return 1;
        }
    } else if (strcmp(cmd, "UPDATE") == 0) {
        if (sscanf(sql, "UPDATE %255s SET %49s = %255s WHERE %49s = %255s", stmt->table_name, stmt->set_col, stmt->set_val, stmt->where_col, stmt->where_val) >= 4) {
            stmt->type = STMT_UPDATE; return 1;
        }
    } else if (strcmp(cmd, "INSERT") == 0) {
        char vals[1024];
        if (sscanf(sql, "INSERT INTO %255s VALUES (%1023[^)])", stmt->table_name, vals) == 2) {
            stmt->type = STMT_INSERT; strcpy(stmt->row_data, vals); return 1;
        }
    }
    return 0;
}

int main(int argc, char *argv[]) {
    char filename[256];
    if (argc >= 2) strcpy(filename, argv[1]);
    else { printf("SQL 파일명: "); scanf("%255s", filename); }
    FILE *f = fopen(filename, "r");
    if (!f) return 1;
    char buf[4096]; int idx = 0, q = 0, ch;
    while ((ch = fgetc(f)) != EOF) {
        if (ch == '-' && !q) { 
            int next_ch = fgetc(f);
            if (next_ch == '-') { while ((ch = fgetc(f)) != EOF && ch != '\n'); continue; }
            else ungetc(next_ch, f);
        }
        if (ch == '\'') q = !q;
        if (ch == ';' && !q) {
            buf[idx] = '\0'; char *s = buf; while (isspace(*s)) s++;
            if (strlen(s) > 0) {
                Statement stmt;
                if (parse_statement(s, &stmt)) {
                    if (stmt.type == STMT_INSERT) execute_insert(&stmt);
                    else if (stmt.type == STMT_SELECT) execute_select(&stmt);
                    else if (stmt.type == STMT_DELETE) execute_delete(&stmt);
                    else if (stmt.type == STMT_UPDATE) execute_update(&stmt);
                }
            }
            idx = 0;
        } else if (idx < 4095) buf[idx++] = (char)ch;
    }
    fclose(f);
    for(int i=0; i<open_table_count; i++) if(open_tables[i].file) fclose(open_tables[i].file);
    return 0;
}