#ifndef TYPES_H
#define TYPES_H

#include <stdio.h>

#define MAX_RECORDS 2000000
#define INITIAL_RECORD_CAPACITY 1024
#define RECORD_SIZE 1024
#define MAX_COLS 15
#define MAX_TABLES 1
#define MAX_UKS 5
#define MAX_SQL_LEN 4096

/* 실행할 Statement 종류입니다. */
typedef enum {
    STMT_INSERT,
    STMT_SELECT,
    STMT_DELETE,
    STMT_UPDATE,
    STMT_UNRECOGNIZED
} StatementType;

typedef enum {
    WHERE_NONE,
    WHERE_EQ,
    WHERE_BETWEEN
} WhereType;

/* 컬럼 제약 타입입니다. (일반 / PK / UK / NN) */
typedef enum {
    COL_NORMAL,
    COL_PK,
    COL_UK,
    COL_NN
} ColumnType;

/* 파서가 생성하는 실행 단위입니다. */
typedef struct {
    StatementType type;          /* SELECT/INSERT/UPDATE/DELETE */
    char table_name[256];        /* 대상 테이블명 */
    char row_data[1024];         /* INSERT VALUES(...) 안쪽 문자열 */
    int select_all;              /* SELECT * 인지 여부 */
    int select_col_count;        /* SELECT col1,col2 형태의 컬럼 수 */
    char select_cols[MAX_COLS][50]; /* SELECT col1,col2 형태의 컬럼명 목록 */
    char set_col[50];            /* UPDATE ... SET col = value */
    char set_val[256];           /* UPDATE ... SET value */
    WhereType where_type;        /* WHERE condition type */
    char where_col[50];          /* WHERE col = value */
    char where_val[256];         /* WHERE value */
    char where_end_val[256];     /* WHERE BETWEEN end value */
} Statement;

/* 컬럼 메타데이터입니다. */
typedef struct {
    char name[50];               /* 컬럼 이름 */
    ColumnType type;              /* COL_NORMAL / PK / UK / NN */
} ColumnInfo;

typedef struct BPlusTree BPlusTree;
typedef struct UniqueIndex UniqueIndex;

/* 테이블 한 개를 메모리에 적재해 관리하는 캐시 구조입니다. */
typedef struct {
    char table_name[256];         /* users 형태 이름 */
    FILE *file;                   /* 현재 열려 있는 CSV 파일 포인터 */
    ColumnInfo cols[MAX_COLS];    /* 헤더 파싱 결과 */
    int col_count;                /* 컬럼 개수 */
    int pk_idx;                   /* PK 컬럼 인덱스, 없으면 -1 */
    int uk_indices[MAX_UKS];      /* UK 컬럼 인덱스 목록 */
    int uk_count;                 /* UK 컬럼 개수 */
    UniqueIndex *uk_indexes[MAX_UKS]; /* UK 컬럼별 B+ Tree 인덱스 */
    BPlusTree *id_index;           /* ID/PK 기반 B+ Tree 인덱스 */
    char **records;                /* slot_id별 레코드 문자열 */
    int *record_active;            /* slot_id별 활성 row 여부 */
    int record_capacity;           /* 동적 slot 배열 용량 */
    int record_count;              /* 할당된 slot 개수 */
    int active_count;              /* 활성 row 개수 */
    int *free_slots;               /* 재사용 가능한 비활성 slot 목록 */
    int free_count;                /* free_slots 개수 */
    int free_capacity;             /* free_slots 배열 용량 */
    int cache_truncated;           /* MAX_RECORDS를 넘어 파일 스캔 fallback이 필요한지 여부 */
    long uncached_start_offset;     /* 캐시되지 않은 첫 CSV row의 파일 offset */
    long next_auto_id;             /* INSERT 시 자동 부여할 다음 ID */
    unsigned long long last_used_seq; /* LRU 계산용 사용 순번 */
} TableCache;

/* Statement 타입으로 토크나이징된 입력을 표현합니다. */
typedef enum {
    TOKEN_EOF,
    TOKEN_ILLEGAL,
    TOKEN_IDENTIFIER,
    TOKEN_STRING,
    TOKEN_NUMBER,
    TOKEN_SELECT,
    TOKEN_INSERT,
    TOKEN_UPDATE,
    TOKEN_DELETE,
    TOKEN_FROM,
    TOKEN_WHERE,
    TOKEN_SET,
    TOKEN_INTO,
    TOKEN_VALUES,
    TOKEN_STAR,
    TOKEN_COMMA,
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_EQ,
    TOKEN_BETWEEN,
    TOKEN_AND,
    TOKEN_SEMICOLON
} SqlTokenType;

/* Lexer가 분리한 한 개의 토큰입니다. */
typedef struct {
    SqlTokenType type;               /* 토큰 타입 */
    char text[256];               /* 원본 문자열 */
} Token;

/* Lexer 내부 상태입니다. */
typedef struct {
    const char *sql;              /* 현재 파싱 중인 SQL 문자열 */
    int pos;                      /* 현재 문자 인덱스 */
} Lexer;

/* 파서(Parser)는 Lexer와 현재 토큰을 묶은 상태입니다. */
typedef struct {
    Lexer lexer;                  /* 실제 토큰 생성기 */
    Token current_token;          /* 다음에 처리할 토큰 */
} Parser;

extern TableCache open_tables[MAX_TABLES];
extern int open_table_count;

#endif


