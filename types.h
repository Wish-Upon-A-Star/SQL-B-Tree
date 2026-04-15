#ifndef TYPES_H
#define TYPES_H

#include <stdio.h>

#define MAX_RECORDS 2000000
#define INITIAL_RECORD_CAPACITY 1024
#define ROW_CACHE_LIMIT 65536
#define PAGE_CACHE_PAGE_SIZE 65536
#define PAGE_CACHE_LIMIT 2048
#define RECORD_SIZE 1024
#define MAX_COLS 15
#define MAX_TABLES 1
#define MAX_UKS 5
#define MAX_WHERE_CONDITIONS 8
#define MAX_SQL_LEN 4096
#define DELTA_COMPACT_BYTES (256 * 1024 * 1024)
#define DELTA_COMPACT_CHECK_INTERVAL 4096

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

typedef struct {
    WhereType type;              /* WHERE condition type */
    char col[50];                /* WHERE column */
    char val[256];               /* WHERE value or BETWEEN start */
    char end_val[256];           /* WHERE BETWEEN end value */
} WhereCondition;

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
    int where_count;             /* WHERE condition count */
    WhereCondition where_conditions[MAX_WHERE_CONDITIONS];
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

typedef enum {
    ROW_STORE_NONE = 0,
    ROW_STORE_CSV = 1,
    ROW_STORE_MEMORY = 2
} RowStoreType;

typedef struct {
    RowStoreType store;           /* CSV page 기반인지 메모리 전용인지 */
    long offset;                  /* CSV 파일에서 row 시작 byte offset */
} RowRef;

typedef struct {
    long page_start;              /* CSV 파일 page 시작 byte offset */
    size_t bytes;                 /* 실제 읽힌 page byte 수 */
    char *data;                   /* PAGE_CACHE_PAGE_SIZE 크기의 page buffer */
    int valid;                    /* page cache entry 사용 여부 */
    unsigned long long last_used; /* PageCache LRU clock */
} PageCacheEntry;

/* 테이블 한 개를 메모리에 적재해 관리하는 캐시 구조입니다. */
typedef struct {
    char table_name[256];         /* users 형태 이름 */
    FILE *file;                   /* 현재 열려 있는 CSV 파일 포인터 */
    FILE *delta_file;             /* append-only delta log writer */
    int delta_batch_open;          /* 현재 delta batch가 B만 쓰고 E 대기 중인지 */
    int delta_ops_since_compact_check; /* delta compaction 크기 확인 주기 */
    long delta_bytes_since_compact; /* 마지막 compaction 이후 delta append 추정 byte 수 */
    ColumnInfo cols[MAX_COLS];    /* 헤더 파싱 결과 */
    int col_count;                /* 컬럼 개수 */
    int pk_idx;                   /* PK 컬럼 인덱스, 없으면 -1 */
    int uk_indices[MAX_UKS];      /* UK 컬럼 인덱스 목록 */
    int uk_count;                 /* UK 컬럼 개수 */
    UniqueIndex *uk_indexes[MAX_UKS]; /* UK 컬럼별 B+ Tree 인덱스 */
    BPlusTree *id_index;           /* ID/PK 기반 B+ Tree 인덱스 */
    char **records;                /* slot_id별 lazy row cache */
    long *row_ids;                  /* PK가 없는 테이블에서도 쓰는 내부 row id */
    RowRef *row_refs;               /* slot_id별 row 위치/저장 방식 참조 */
    long *row_offsets;              /* CSV에서 row가 시작되는 파일 offset */
    unsigned char *row_store;       /* RowStoreType: CSV offset 기반인지 메모리 전용인지 */
    unsigned char *row_cached;      /* records[slot_id]가 현재 메모리에 올라와 있는지 */
    unsigned long long *row_cache_seq; /* slot row cache LRU 계산용 */
    PageCacheEntry page_cache[PAGE_CACHE_LIMIT]; /* CSV page 단위 캐시 */
    int page_cache_count;           /* 현재 유효 page cache entry 수 */
    unsigned long long page_cache_clock; /* PageCache LRU clock */
    int *record_active;            /* slot_id별 활성 row 여부 */
    int record_capacity;           /* 동적 slot 배열 용량 */
    int record_count;              /* 할당된 slot 개수 */
    int active_count;              /* 활성 row 개수 */
    int cached_record_count;        /* 현재 records[]에 올라와 있는 row 문자열 수 */
    unsigned long long row_cache_clock; /* row cache LRU clock */
    int row_cache_evict_cursor;     /* row cache eviction 시작 위치 */
    int *free_slots;               /* 재사용 가능한 비활성 slot 목록 */
    int free_count;                /* free_slots 개수 */
    int free_capacity;             /* free_slots 배열 용량 */
    int cache_truncated;           /* MAX_RECORDS를 넘어 파일 스캔 fallback이 필요한지 여부 */
    long uncached_start_offset;     /* 캐시되지 않은 첫 CSV row의 파일 offset */
    long *tail_pk_ids;              /* 캐시 밖 PK row의 id 목록 */
    long *tail_offsets;             /* tail_pk_ids와 대응되는 CSV 파일 offset */
    int tail_count;                 /* 캐시 밖 PK offset 개수 */
    int tail_capacity;              /* tail offset 배열 용량 */
    long next_auto_id;             /* INSERT 시 자동 부여할 다음 ID */
    long next_row_id;              /* PK가 없는 테이블의 delta/index 식별자 */
    long append_offset;            /* 다음 CSV append가 시작될 byte offset */
    int snapshot_loaded;           /* 유효한 .idx snapshot으로 현재 상태를 복구했는지 */
    int snapshot_dirty;            /* INSERT/UPDATE/DELETE 이후 .idx 재저장이 필요한지 */
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


