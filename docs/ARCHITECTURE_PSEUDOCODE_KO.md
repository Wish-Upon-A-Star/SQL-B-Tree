# SQL-B-Tree 구조 의사코드

이 문서는 코드를 처음부터 끝까지 읽기 위한 문서가 아니다.

목적은 세 가지다.

1. 지금 프로그램이 어떤 모듈 경계로 움직이는지 빠르게 이해한다.
2. 회의 중 결정이 바뀌었을 때 어느 모듈을 바꿔야 하는지 찾는다.
3. 구현 세부보다 "어떤 방식으로 풀었는가"를 발표용 언어로 정리한다.

## 1. 지금 프로그램이 푸는 문제

- 입력은 SQL 파일이다.
- 저장소는 DB 파일이 아니라 CSV 파일이다.
- SQL을 파싱해 메모리에 올린 테이블 캐시를 수정하거나 조회한다.
- `PK(id)`와 `UK` 컬럼은 인덱스로 처리한다.
- 현재 핵심 차별점은 `SELECT ... WHERE id = ...` 와 `UK 조회`를 B+ tree로 빠르게 처리한다는 점이다.
- 인덱스가 없는 일반 컬럼은 선형 탐색으로 남겨서, "인덱스 경로 vs 스캔 경로"가 분명하게 드러난다.

## 2. 전체 프로그램을 보는 관점

### 한 줄 요약

`SQL 파일 읽기 -> 문장 분리 -> 토큰화 -> Statement 생성 -> 테이블 캐시 확보 -> 인덱스/스캔으로 실행 -> 필요 시 CSV 재작성`

### 역할 기준 모듈

- `main.c`
  - SQL 파일을 읽고 문장을 순서대로 실행한다.
  - `--benchmark` 모드도 여기서 시작한다.
- `lexer.c`
  - SQL 문자열을 토큰 단위로 자른다.
- `parser.c`
  - 토큰 흐름을 `Statement` 구조체로 바꾼다.
- `executor.c`
  - 실제 테이블을 열고, 메모리 캐시를 유지하고, CRUD를 수행한다.
  - 여기서 B+ tree 인덱스를 사용하거나 다시 만든다.
- `bptree.c`
  - 숫자 PK용 B+ tree와 문자열 UK용 B+ tree를 제공한다.

## 3. 프로그램 전체 흐름 의사코드

```text
function main(args):
    if args starts with "--benchmark":
        run_bplus_benchmark()
        end

    filename = choose_sql_file(args)
    sql_text_stream = open(filename)

    while reading characters:
        remove "-- comment"
        split by ';' outside quotes
        for each completed statement_text:
            execute_sql_text(statement_text)

    close_all_tables()
```

```text
function execute_sql_text(sql):
    trim BOM and spaces
    if empty:
        return

    stmt = parse_statement(sql)
    if parse failed:
        print error
        return

    switch stmt.type:
        INSERT -> execute_insert(stmt)
        SELECT -> execute_select(stmt)
        UPDATE -> execute_update(stmt)
        DELETE -> execute_delete(stmt)
```

## 4. 핵심 구조체를 보는 법

### Statement

SQL 한 문장을 "실행 가능한 계획의 최소 단위"로 바꾼 결과물이다.

```text
struct Statement:
    type
    table_name
    row_data          // INSERT VALUES 내부 문자열
    select_all
    select_cols[]
    set_col, set_val
    where_col, where_val
```

이 구조체를 기준으로 보면 파서의 목표가 명확해진다.

- Lexer의 목표: 토큰을 잘라내는 것
- Parser의 목표: `Statement`를 채우는 것
- Executor의 목표: `Statement`를 실제 데이터 변화로 연결하는 것

### TableCache

이 프로그램에서 가장 중요한 운영 구조체다.
CSV 파일 하나를 메모리에 적재한 뒤, 레코드와 인덱스를 함께 관리한다.

```text
struct TableCache:
    table_name
    file
    cols[]
    col_count

    pk_idx
    uk_indices[]
    uk_count

    id_index          // PK(id)용 숫자 B+ tree
    uk_indexes[]      // UK 컬럼용 문자열 B+ tree

    records[]         // 실제 CSV row 문자열 배열
    record_count
    next_auto_id
    last_used_seq     // LRU 교체용
```

이 구조체를 기준으로 보면, 현재 프로그램은 "테이블 파일"보다 "테이블 캐시 + 인덱스 묶음" 중심으로 움직인다.

### BPlusTree / BPlusStringTree

현재 B+ tree는 두 종류다.

- 숫자 키용: `id(PK)` 조회
- 문자열 키용: `email(UK)` 같은 UK 조회

핵심은 둘 다 결국 `key -> row_index` 를 찾는 용도라는 점이다.

```text
numeric tree:
    key = long
    value = row_index

string tree:
    key = char*
    value = row_index
```

## 5. 파싱 계층 의사코드

### Lexer

```text
function get_next_token(sql, pos):
    skip spaces

    if end:
        return EOF

    if current char is one of "* , = ( ) ;":
        return punctuation token

    if current char starts with quote:
        read until closing quote
        return STRING token

    if current char starts with alphabet or underscore:
        read identifier
        if keyword:
            return keyword token
        else:
            return IDENTIFIER token

    if current char starts with digit:
        read number-like token
        return NUMBER token

    return ILLEGAL
```

### Parser

```text
function parse_statement(sql):
    init lexer
    read first token

    switch first token:
        SELECT -> parse_select()
        INSERT -> parse_insert()
        UPDATE -> parse_update()
        DELETE -> parse_delete()
        else fail
```

```text
function parse_select():
    read selected columns or '*'
    read FROM table
    optionally read WHERE col = value
    return Statement(type=SELECT)
```

```text
function parse_insert():
    read INTO table
    read VALUES(...)
    keep inside of parentheses as raw row_data
    return Statement(type=INSERT)
```

포인트는 이거다.

- Parser는 "복잡한 SQL 엔진"이 아니다.
- 대신 이번 프로젝트 범위에서 필요한 SQL을 아주 직접적으로 `Statement` 구조로 떨어뜨린다.
- 그래서 발표에서는 "간단한 SQL subset을 안정적으로 실행하는 구조"라고 설명하면 된다.

## 6. 실행 계층 의사코드

### get_table: 테이블 캐시 진입점

```text
function get_table(name):
    if table already cached:
        touch LRU sequence
        return cached table

    open name.csv
    if cache slot is full:
        evict least recently used table

    load_table_contents()
    return loaded table
```

현재는 동시에 최대 1개 테이블만 캐시한다.
즉, 메모리를 단순하게 유지하는 대신 다중 테이블 캐시 전략은 아직 단순화돼 있다.

### load_table_contents: CSV -> 메모리 + 인덱스

```text
function load_table_contents(table, file):
    reset empty cache
    read header
    parse column names and constraints
    identify PK and UK columns

    for each row in CSV:
        validate row length
        parse PK if exists
        append raw row into records[]

    rebuild_id_index()
    rebuild_uk_indexes()
```

여기서 중요한 설계 포인트:

- 파일을 읽는 동안 트리에 한 줄씩 넣지 않는다.
- 먼저 메모리에 다 올린다.
- 이후 정렬된 key-row 목록을 만들어 bulk-build 한다.

즉, `로드 단계는 대량 적재 최적화`, `INSERT 단계는 점진 삽입`으로 역할이 분리돼 있다.

### execute_select: 어떤 조회 경로를 탈지 결정

```text
function execute_select(stmt):
    table = get_table(stmt.table_name)
    resolve where column index
    resolve selected columns

    if WHERE column is PK(id):
        print "index path"
        row_index = search numeric B+ tree
        print matching row
        return

    if WHERE column is UK:
        print "index path"
        row_index = search string B+ tree
        print matching row
        return

    print "linear scan"
    for every row in memory:
        if condition matches:
            print row
```

발표에서 가장 강조하기 좋은 함수다.
이 함수 하나에 현재 프로젝트의 메시지가 다 들어 있다.

- PK 조회는 B+ tree
- UK 조회도 B+ tree
- 일반 컬럼은 linear scan

즉, "어디에 인덱스를 붙였고, 어디는 아직 스캔으로 남겨 두었는가"가 분명하다.

### execute_insert: 점진 삽입 경로

```text
function execute_insert(stmt):
    table = get_table(stmt.table_name)
    parse row_data into values[]

    build_insert_row():
        decide auto id if needed
        check PK duplicate with id_index
        check UK duplicate with uk_indexes
        build normalized CSV row string

    append_record_memory():
        append row into records[]
        insert PK into id_index
        insert UK into uk_indexes

    append row into file
    if file append fails:
        rollback memory/index
```

핵심 메시지:

- INSERT는 "파일만 쓰는 작업"이 아니라
- `메모리 row + PK index + UK index + 파일 append` 를 함께 유지하는 작업이다.

### execute_update / execute_delete: 전체 재구성 경로

현재 UPDATE와 DELETE는 더 단순한 전략을 쓴다.

```text
function execute_update(stmt):
    find matching rows
    rebuild changed row strings in memory
    rebuild UK indexes
    rewrite whole CSV file
    on failure, restore old memory
```

```text
function execute_delete(stmt):
    mark rows to delete
    compact records[] in memory
    rebuild PK index
    rebuild UK indexes
    rewrite whole CSV file
    on failure, restore old memory
```

즉, 현재 구조는:

- `SELECT / INSERT` 는 인덱스를 직접 활용하는 경로
- `UPDATE / DELETE` 는 안전하게 전체 재구성하는 경로

이 차이는 발표에서 오히려 장점이 될 수 있다.
왜냐하면 "지금 어디까지 최적화했고, 어디는 아직 단순 전략으로 두었는지"가 정직하게 보이기 때문이다.

## 7. 현재 B+ tree 구조를 이해하는 단순 모델

### 핵심 아이디어

```text
internal node:
    keys = 경계값
    children = 다음 노드 포인터

leaf node:
    keys = 실제 검색 키
    values = row_index
    next = 오른쪽 leaf 연결
```

즉, internal node는 "어느 방향으로 내려갈지"를 결정하고,
leaf node는 "실제 row 위치"를 들고 있다.

### 숫자 PK tree

```text
search(id):
    root에서 시작
    internal node의 경계값과 비교하며 아래로 이동
    leaf에 도착하면 key == id 찾기
    row_index 반환
```

### 문자열 UK tree

```text
search(uk_string):
    root에서 시작
    문자열 비교로 child 선택
    leaf에서 정확한 문자열 key 찾기
    row_index 반환
```

### 삽입 시 일어나는 일

```text
insert(key, row_index):
    leaf까지 내려간다
    key를 정렬 위치에 끼워 넣는다

    if leaf overflow:
        leaf split
        오른쪽 leaf의 첫 key를 부모에 올린다

    if parent overflow:
        internal split
        분리 기준 key를 더 위로 올린다

    if root overflow:
        새 root를 만든다
```

이 문장만 이해해도 현재 `bptree_insert()` 의 큰 흐름을 따라갈 수 있다.

### 로드 시 bulk-build를 쓰는 이유

```text
when loading CSV:
    gather all (key, row_index)
    sort pairs if needed
    build leaf level first
    connect leaf nodes left -> right
    build parent levels upward
```

이 방식은 대량 CSV를 열 때 매우 중요하다.
한 줄씩 삽입하면 split이 반복되지만,
bulk-build는 정렬된 입력으로 leaf와 internal level을 한 번에 만든다.

## 8. 발표에서 강조할 만한 구조 포인트

### 포인트 1. SQL 엔진을 "작은 파이프라인"으로 단순화했다

- SQL 파일 읽기
- Lexer
- Parser
- Executor
- Storage / Index

즉, 복잡한 DBMS 전체 대신 지금 필요한 흐름만 남겼다.

### 포인트 2. 저장은 CSV지만 조회는 인덱스 기반으로 바꿨다

- 파일 형식은 단순하다.
- 하지만 메모리에 올릴 때 PK/UK 인덱스를 만들어 조회 경로를 바꾼다.

### 포인트 3. 읽기 최적화와 쓰기 단순화를 분리했다

- SELECT는 빠른 조회 경로를 보여준다.
- UPDATE/DELETE는 전체 재구성으로 안정성을 우선한다.

### 포인트 4. B+ tree를 "row 위치 찾기 기계"로 사용한다

- key 자체를 저장하는 게 목적이 아니다.
- 최종 목적은 `row_index`를 찾는 것이다.
- 그 후 실제 레코드는 `records[]` 에서 가져온다.

## 9. 회의 때 보면 좋은 질문

- PK 말고 어떤 컬럼까지 인덱스 대상으로 둘 것인가?
- UPDATE/DELETE도 점진 갱신으로 바꿀 것인가?
- 지금 `records[] + row_index` 구조가 발표 메시지에 충분히 명확한가?
- bulk-build와 incremental insert를 둘 다 설명할 것인가?
- "DB 파일 엔진"이 아니라 "메모리 캐시 + 인덱스 계층"으로 소개하는 편이 더 낫지 않은가?

## 10. 한 장 요약

```text
입력:
    SQL 파일

해석:
    lexer -> parser -> Statement

실행:
    executor -> table cache -> index or scan path

저장:
    records[] in memory + CSV file

인덱스:
    PK(id) -> numeric B+ tree
    UK     -> string B+ tree
    others -> linear scan
```

이 요약을 중심으로 다이어그램과 발표를 맞추면,
코드를 다 읽지 않아도 현재 구조와 선택의 이유를 계속 추적할 수 있다.
