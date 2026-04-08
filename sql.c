#include <stdio.h>
#include <string.h>
#include <stdlib.h>

// 1. SQL 명령어 종류를 구분하기 위한 열거형
typedef enum { 
    STMT_INSERT, 
    STMT_SELECT, 
    STMT_UNRECOGNIZED 
} StatementType;

// 2. 파싱된 SQL 데이터를 담는 구조체
typedef struct {
    StatementType type;
    char table_name[256];
    char row_data[1024]; // INSERT할 때 사용할 데이터
} Statement;

// [추가할 함수 1] 파일이 없으면 새로 만들고 스키마(헤더)를 작성하는 함수
void create_table_if_not_exists(const char* table_name, const char* schema_header) {
    char filename[300];
    sprintf(filename, "%s.csv", table_name);

    // "r" 모드로 열기를 시도해서 파일이 이미 존재하는지 확인합니다.
    FILE *file = fopen(filename, "r");
    
    if (file == NULL) {
        // 파일이 없다면 "w"(쓰기) 모드로 새로 생성합니다.
        file = fopen(filename, "w");
        if (file != NULL) {
            // 첫 줄에 스키마(컬럼명)를 적어줍니다.
            fprintf(file, "%s\n", schema_header);
            fclose(file);
            printf("[시스템] '%s' 테이블이 없어 새로 생성했습니다. (스키마: %s)\n", filename, schema_header);
        }
    } else {
        // 파일이 이미 존재하면 그냥 닫습니다. (기존 데이터 유지)
        fclose(file);
    }
}

// [추가할 함수 2] 프로그램 시작 시 데이터베이스(테이블들)를 준비하는 함수
void init_database() {
    printf("--- 데이터베이스 초기화 중 ---\n");
    // 사용할 테이블과 스키마 구조를 미리 정의해 둡니다.
    create_table_if_not_exists("users", "id,name,age");
    create_table_if_not_exists("products", "id,product_name,price");
    printf("--- 데이터베이스 준비 완료 ---\n\n");
}



// =================================================================
// [함수 1] SQL 문장 분석 (Parser)
// =================================================================
int parse_statement(char *sql, Statement *stmt) {
    char command[20];
    
    // 첫 번째 단어(명령어)만 먼저 읽어옵니다.
    sscanf(sql, "%s", command);

    if (strcmp(command, "INSERT") == 0) {
        stmt->type = STMT_INSERT;
        // 형식: INSERT INTO 테이블명 VALUES (데이터)
        // %s 로 테이블명을 받고, %[^)] 로 ')'가 나올 때까지의 모든 문자열을 row_data에 저장합니다.
        sscanf(sql, "INSERT INTO %s VALUES (%[^)])", stmt->table_name, stmt->row_data);
        return 1; // 파싱 성공
    } 
    else if (strcmp(command, "SELECT") == 0) {
        stmt->type = STMT_SELECT;
        // 형식: SELECT * FROM 테이블명
        sscanf(sql, "SELECT * FROM %s", stmt->table_name);
        return 1; // 파싱 성공
    }

    return 0; // 알 수 없는 명령어 (파싱 실패)
}

// =================================================================
// [함수 2] INSERT 실행 (파일에 데이터 저장)
// =================================================================
void execute_insert(Statement *stmt) {
    char filename[300];
    sprintf(filename, "%s.csv", stmt->table_name); // 예: users.csv

    // "a" 모드 (Append): 파일이 없으면 새로 만들고, 있으면 맨 끝에 내용을 추가합니다.
    FILE *file = fopen(filename, "a");
    if (file == NULL) {
        printf("[오류] %s 테이블 파일을 열 수 없습니다.\n", filename);
        return;
    }

    // 파일에 데이터 쓰고 줄바꿈
    fprintf(file, "%s\n", stmt->row_data);
    fclose(file);
    
    printf("[성공] %s 테이블에 데이터를 저장했습니다. (%s)\n", stmt->table_name, stmt->row_data);
}

// =================================================================
// [함수 3] SELECT 실행 (파일에서 데이터 읽어오기)
// =================================================================
void execute_select(Statement *stmt) {
    char filename[300];
    sprintf(filename, "%s.csv", stmt->table_name); 

    // "r" 모드 (Read): 읽기 전용으로 파일을 엽니다.
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("[알림] %s 테이블에 데이터가 없습니다. (파일 없음)\n", filename);
        return;
    }

    printf("=== [%s] 테이블 조회 결과 ===\n", stmt->table_name);
    char line[1024];
    // 파일의 끝(EOF)에 도달할 때까지 한 줄씩 읽어서 출력합니다.
    while (fgets(line, sizeof(line), file) != NULL) {
        printf("- %s", line);
    }
    printf("===============================\n");

    fclose(file);
}

// =================================================================
// [함수 4] 실행 매니저
// =================================================================
void execute_statement(Statement *stmt) {
    if (stmt->type == STMT_INSERT) {
        execute_insert(stmt);
    } else if (stmt->type == STMT_SELECT) {
        execute_select(stmt);
    }
}

// =================================================================
// [메인 함수] 커맨드 라인 입력 및 전체 흐름 제어
// =================================================================
// =================================================================
// [메인 함수] scanf로 파일명 입력받기
// =================================================================
// =================================================================
// [메인 함수] 커맨드 라인 인자 우선 처리, 없으면 scanf로 입력받기
// =================================================================
int main(int argc, char *argv[]) {
    char filename[256];

    // 1. 데이터베이스 스키마 및 테이블 준비
    init_database(); 

    // 2. 실행 시 파일명을 인자로 주었는지 확인
    if (argc >= 2) {
        // 인자값이 있는 경우 (예: ./sql_processor queries.txt)
        // argv[1]에 있는 파일 이름을 filename 배열로 복사합니다.
        strcpy(filename, argv[1]); 
    } else {
        // 인자값이 없는 경우 (예: ./sql_processor 만 입력한 경우)
        printf("실행할 SQL 파일 이름을 입력하세요 (예: queries.txt): ");
        scanf("%s", filename);
    }

    // 3. 결정된 파일 이름으로 파일 열기
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        printf("[오류] '%s' 파일을 찾을 수 없습니다. 파일 이름과 확장자를 확인해 주세요.\n", filename);
        return 1;
    }

    char line[1024];
    Statement stmt;

    printf("\n--- [%s] 파일 읽기 시작 ---\n", filename);

    // 4. 파일에서 한 줄씩 읽어오기
    while (fgets(line, sizeof(line), file) != NULL) {
        
        // 줄바꿈과 세미콜론 제거
        line[strcspn(line, ";\n")] = '\0';

        // 빈 줄이면 건너뛰기
        if (strlen(line) == 0) continue;

        printf("\n[실행 중인 SQL] %s;\n", line);

        // 파싱 및 실행
        if (parse_statement(line, &stmt) == 1) {
            execute_statement(&stmt);
        } else {
            printf("[오류] 지원하지 않거나 잘못된 SQL 문법입니다.\n");
        }
    }

    // 5. 파일 닫기
    fclose(file);
    return 0;
}