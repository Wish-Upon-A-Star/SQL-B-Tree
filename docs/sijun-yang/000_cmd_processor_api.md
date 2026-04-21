# CmdProcessor 요청 처리 API 설계 계획

## 1. 목적

이번 단계의 목표는 기존 DB 코드와 실행 흐름을 수정하지 않고, 나중에 CLI REPL, 네트워크 API, 기타 외부 API가 공통으로 사용할 요청 처리 인터페이스를 정의하는 것이다.

목표 구조는 아래와 같다.

```text
외부 진입점
        -> CmdProcessor API
        -> DB 구현체
```

현재 단계에서는 마지막 `DB 구현체` 연결을 수행하지 않는다. 현재 SQL parser/executor를 `CmdProcessor`에 연결하는 작업은 추후 DB 구현 변경 작업에서 함께 진행한다.

## 2. 이번 단계의 범위

이번 단계에서 하는 일:

- `CmdProcessor` API 계약을 설계한다.
- 요청/응답 구조체를 정의한다.
- processor 공용 context와 요청별 context의 수명 규칙을 정한다.
- 동시 호출 가능성을 `CmdProcessor` API 요구사항으로 명시한다.

이번 단계에서 하지 않는 일:

- 기존 `main.c` 수정.
- 기존 `parser.c`, `executor.c`, `lexer.c`, `bptree.c` 수정.
- 기존 SQL 실행 흐름 분리.
- 현재 SQL parser/executor를 `CmdProcessor`에 연결.
- 기존 CLI 파일 실행 방식을 새 API로 교체.
- DB 내부 동시성 구현.

즉, 이번 단계의 산출물은 기존 코드에 연결되지 않은 독립 요청 처리 API다.

## 3. 설계 방향

첫 공개 요청 형식은 SQL 문자열 중심으로 둔다.

이유는 다음과 같다.

- 나중에 현재 SQL parser/executor와 연결하기 쉽다.
- 여러 외부 진입점이 같은 요청 모델을 쓸 수 있다.
- DB 내부 구현이 나중에 구조화 CRUD API로 바뀌더라도 외부 요청 계약을 유지할 수 있다.

다만 `CmdProcessor`는 SQL 문자열만 처리하는 전용 인터페이스로 고정하지 않는다. `PING`, 추후 structured request 같은 확장을 받을 수 있도록 요청 타입을 둔다.

`CmdProcessor`는 외부 진입점과 DB 구현체 사이의 사용자 요청 처리 계층이다.

- DB 구현체는 SQL 실행과 저장소 일관성을 책임진다.
- 사용자 요청 ID, timeout, client별 제한 같은 외부 요청 처리는 `CmdProcessor`가 책임진다.
- TCP socket 연결 종료, HTTP connection 관리, REPL loop 종료 같은 transport lifecycle은 외부 진입점이 책임진다.
- 이 프로젝트의 v1 API는 transaction, cursor, prepared statement 같은 요청 간 상태를 제공하지 않으므로 session 개념을 의도적으로 제외한다.
- 상태는 processor 인스턴스 단위의 공용 context와 요청 1개 단위의 request context만 둔다.

## 4. API 계약

공용 API 헤더는 `cmd_processor.h`로 둔다.

확정 요청 모델:

```c
typedef enum {
    CMD_REQUEST_SQL,
    CMD_REQUEST_PING
} CmdRequestType;

typedef enum {
    CMD_STATUS_OK,
    CMD_STATUS_BAD_REQUEST,
    CMD_STATUS_SQL_TOO_LONG,
    CMD_STATUS_PARSE_ERROR,
    CMD_STATUS_PROCESSING_ERROR,
    CMD_STATUS_BUSY,
    CMD_STATUS_TIMEOUT,
    CMD_STATUS_INTERNAL_ERROR
} CmdStatusCode;

typedef enum {
    CMD_BODY_NONE,
    CMD_BODY_TEXT,
    CMD_BODY_JSON
} CmdBodyFormat;

typedef struct {
    char request_id[64];
    CmdRequestType type;
    char *sql;
} CmdRequest;

typedef struct {
    char request_id[64];
    CmdStatusCode status;
    int ok;
    int row_count;
    int affected_count;
    CmdBodyFormat body_format;
    char *body;
    size_t body_len;
    char *error_message;
} CmdResponse;

typedef struct {
    const char *name;
    size_t max_sql_len;
    size_t request_buffer_count;
    size_t response_body_capacity;
    void *shared_state;
} CmdProcessorContext;

typedef struct CmdProcessor {
    CmdProcessorContext *context;
    int (*acquire_request)(CmdProcessorContext *context,
                           CmdRequest **out_request);
    int (*process)(CmdProcessorContext *context,
                   CmdRequest *request,
                   CmdResponse **out_response);
    int (*make_error_response)(CmdProcessorContext *context,
                               const char *request_id,
                               CmdStatusCode status,
                               const char *error_message,
                               CmdResponse **out_response);
    void (*release_request)(CmdProcessorContext *context,
                            CmdRequest *request);
    void (*release_response)(CmdProcessorContext *context,
                             CmdResponse *response);
    void (*shutdown)(CmdProcessorContext *context);
} CmdProcessor;
```

외부 진입점이 직접 호출하는 확정 wrapper API:

```c
const char *cmd_status_to_string(CmdStatusCode status);

int cmd_processor_acquire_request(CmdProcessor *processor,
                                  CmdRequest **out_request);
CmdStatusCode cmd_processor_set_sql_request(CmdProcessor *processor,
                                            CmdRequest *request,
                                            const char *request_id,
                                            const char *sql);
CmdStatusCode cmd_processor_set_ping_request(CmdProcessor *processor,
                                             CmdRequest *request,
                                             const char *request_id);
int cmd_processor_process(CmdProcessor *processor,
                          CmdRequest *request,
                          CmdResponse **out_response);
int cmd_processor_make_error_response(CmdProcessor *processor,
                                      const char *request_id,
                                      CmdStatusCode status,
                                      const char *error_message,
                                      CmdResponse **out_response);
void cmd_processor_release_request(CmdProcessor *processor,
                                   CmdRequest *request);
void cmd_processor_release_response(CmdProcessor *processor,
                                    CmdResponse *response);
void cmd_processor_shutdown(CmdProcessor *processor);
```

mock 구현체 생성 API:

```c
int mock_cmd_processor_create(const CmdProcessorContext *base_context,
                              CmdProcessor **out_processor);
```

`CmdProcessor.process()`의 책임:

- `CmdRequest`를 받아 처리한다.
- 성공/실패를 processor가 소유한 `CmdResponse`로 반환한다.
- 호출자가 넘긴 `request_id`를 응답에 보존한다.
- 내부 처리 방식은 구현체가 결정한다.

`process()` 반환값과 `CmdResponse.status`의 의미:

- `process()` 반환값은 API 호출 자체가 수행됐는지를 나타낸다.
- `process()`가 `0`을 반환하면 `response.status`와 `response.ok`가 요청 처리 결과의 기준이다.
- `process()`가 `0`이 아닌 값을 반환하면 `CmdResponse`를 신뢰할 수 없는 API 사용 오류 또는 치명적 내부 오류로 본다.
- SQL 문법 오류, SQL 길이 초과, busy, timeout 같은 정상적인 요청 실패는 `process()`가 `0`을 반환하고 `response.status`로 표현한다.

`CmdRequest.sql` 규칙:

- v1 SQL 요청은 NUL 종료 문자열만 지원한다.
- SQL 문자열은 `cmd_processor_set_sql_request()`가 processor 소유 버퍼로 복사한다.
- 외부 진입점은 자신의 JSON parser, socket buffer, REPL 입력 버퍼 주소를 `CmdRequest.sql`에 직접 넣지 않는다.
- SQL 길이 검증은 복사 시점에 `processor->context->max_sql_len` 기준으로 수행한다.
- SQL 길이가 제한을 넘으면 setter는 `CMD_STATUS_SQL_TOO_LONG`을 반환한다.
- setter가 `CMD_STATUS_OK`가 아닌 값을 반환하면 외부 진입점은 `cmd_processor_make_error_response()`로 processor-owned 오류 응답을 만들 수 있다.
- `cmd_processor_make_error_response()`는 processor 구현체의 `make_error_response` callback을 통해 response slot을 확보한다. 따라서 frontend validation 오류 응답도 일반 `CmdResponse`와 같은 release 규칙을 따른다.

`CmdResponse.body` 규칙:

- `body_format`은 `body`의 의미를 명시한다.
- `CMD_BODY_NONE`이면 `body`는 `NULL`이고 `body_len`은 `0`이다.
- `CMD_BODY_TEXT`는 CLI 출력, plain text 결과, human-readable 결과에 쓴다.
- `CMD_BODY_JSON`은 TCP JSONL, HTTP 같은 구조화 응답 변환에 쓴다. 이때 `body`는 JSON fragment 또는 JSON value 문자열이다.
- `body_len`은 `body`가 `NULL`이 아닐 때 payload byte 길이다. `body`가 NUL 종료 문자열이더라도 호출자는 `body_len`을 우선한다.

해제 규칙:

- `CmdRequest`, `CmdResponse`, 그리고 그 안의 `sql`, `body`, `error_message` 버퍼 소유권은 모두 `CmdProcessor`에 있다.
- 외부 진입점은 request/response 객체와 내부 버퍼를 직접 `malloc()`/`free()`하지 않는다.
- 외부 진입점은 `cmd_processor_acquire_request()`로 요청 객체를 얻고 setter API로 요청 내용을 복사한다.
- 외부 진입점은 요청 처리가 끝난 뒤 `release_request()`와 `release_response()`를 호출한다.
- release API는 processor 내부 request/response slot 또는 heap buffer를 재사용 가능 상태로 되돌린다.
- processor 구현체는 buffer pool, per-request heap allocation, thread-local buffer 중 하나를 자유롭게 선택할 수 있다. 단, public 소유권은 항상 processor-owned다.

frontend별 기본 호출 흐름:

```text
REPL CLI
  -> 한 줄 입력
  -> cmd_processor_acquire_request()
  -> cmd_processor_set_sql_request()
  -> cmd_processor_process()
  -> CmdResponse를 터미널에 출력
  -> cmd_processor_release_response()
  -> cmd_processor_release_request()

TCP JSONL
  -> JSON line 파싱
  -> cmd_processor_acquire_request()
  -> JSON id/op/sql을 CmdRequest 버퍼로 복사
  -> cmd_processor_process()
  -> CmdResponse를 JSON line으로 직렬화
  -> release

HTTP
  -> HTTP request body 파싱
  -> cmd_processor_acquire_request()
  -> route/body를 CmdRequest 버퍼로 복사
  -> cmd_processor_process()
  -> CmdResponse를 HTTP response로 직렬화
  -> release
```

## 5. Context 범위와 수명

context는 두 종류로 구분한다.

| 구분 | 구조체 | 수명 | 소유자 | 용도 |
|---|---|---|---|---|
| 공용 processor context | `CmdProcessorContext` | processor 생성부터 shutdown까지 | DB 구현체 또는 mock 구현체 | 설정, lock, thread pool, DB 상태 같은 공유 상태 보관 |
| 요청별 context | `CmdRequest` | acquire부터 release까지 | CmdProcessor | request id, 요청 타입, SQL 전달 |

공용 processor context 규칙:

- `CmdProcessorContext`는 프로세스 전체 전역 singleton이 아니다.
- 실행 프로그램이 원하면 전역 변수로 둘 수 있지만, API는 전역 singleton을 요구하지 않는다.
- processor 인스턴스 1개는 context 1개를 가진다.
- processor 구현체 또는 인스턴스마다 고유한 context를 가질 수 있다.
- 예를 들어 TCP용 processor와 CLI용 processor를 별도 구현체 또는 별도 인스턴스로 만들면 둘의 context는 다를 수 있다.
- 여러 호출자는 같은 processor 인스턴스와 같은 공용 context를 공유할 수 있다.
- `shared_state`는 DB 구현체 전용 포인터다. 외부 진입점 구현체는 이를 해석하거나 캐스팅하지 않는다.
- `name`, `max_sql_len`, `request_buffer_count`, `response_body_capacity`처럼 외부 진입점도 알아야 하는 최소 공통 설정만 공개 필드로 둔다.

요청별 context 규칙:

- `CmdRequest`는 요청마다 `cmd_processor_acquire_request()`로 얻는다.
- `CmdRequest` 안의 `sql` 포인터는 processor 소유 버퍼를 가리킨다.
- `CmdRequest`는 `cmd_processor_release_request()` 호출 전까지 유효하다.
- `CmdProcessor` 구현체가 요청을 비동기로 처리하거나 저장해야 한다면 같은 processor 소유권 안에서 필요한 문자열과 메타데이터를 보존해야 한다.
- v1은 요청 간 상태를 유지하지 않는다. 이전 요청의 결과나 연결 상태를 다음 요청 처리 계약에 포함하지 않는다.

session 제외 메모:

- 이 프로젝트는 가벼운 SQL processor로 시작하며, v1은 단순 `INSERT`, `SELECT` 중심의 요청/응답 모델을 목표로 한다.
- transaction, cursor, prepared statement, user auth, session-level setting을 제공하지 않으므로 API에 `session_id`를 넣지 않는다.
- TCP의 socket connection, HTTP keep-alive connection, REPL 실행 loop는 transport/frontend의 lifecycle이지 `CmdProcessor`의 요청 처리 context가 아니다.
- 나중에 요청 간 상태가 실제로 필요해지면 그때 `client_context_id` 같은 optional grouping key를 별도 확장으로 추가한다.

이 구분을 두면 아직 DB 구현체가 없어도 외부 진입점 구현체는 `CmdProcessor` 인스턴스만 받아 요청을 전달할 수 있다. 나중에 실제 DB 구현체는 같은 인터페이스에 자신의 공용 context를 꽂아 사용할 수 있다.

## 6. 동시성 요구사항

동시성은 외부 진입점의 단순 구현 세부사항이 아니라 `CmdProcessor` API의 요구사항이다.

요구사항:

- `CmdProcessor.process()`는 여러 호출 지점에서 동시에 호출될 수 있다고 가정한다.
- 단일 CLI REPL은 보통 단일 호출 흐름이지만, 네트워크 API나 외부 API 구현체는 여러 요청을 동시에 호출할 수 있다.
- 이 동시 호출을 싱글스레드 큐로 처리할지, mutex로 보호할지, worker pool로 병렬 처리할지, read/write lock으로 분리할지는 DB 구현체의 책임이다.
- 외부 진입점 구현체는 특정 DB 동시성 모델을 강제하지 않는다.
- 한 요청 처리가 끝나기 전에 다른 요청이 `process()`로 들어올 수 있다.
- 구현체는 내부적으로 싱글스레드 처리든 멀티스레드 처리든 상관없이 data race, crash, response 메모리 오염이 발생하지 않게 해야 한다.
- 병렬 실행을 지원하지 않는 구현체는 내부에서 직렬화하거나 `CMD_STATUS_BUSY` 또는 `CMD_STATUS_TIMEOUT`을 반환해야 한다.

계약:

```text
CmdProcessor 구현체는 process()가 동시 호출되어도 안전해야 한다.
구현체는 자신의 동시성 정책을 내부적으로 보장하거나, 명시적으로 BUSY/TIMEOUT 같은 상태를 반환해야 한다.
외부 진입점 구현체는 DB 실행 직렬화 여부를 자체적으로 결정하지 않는다.
DB 구현체는 실행 요청이 동시에 들어올 수 있음을 알고, 내부에서 직렬화/병렬화/거절 중 하나를 안전하게 선택해야 한다.
```

## 7. 파일/모듈 계획

이번 단계에서 추가할 파일은 아래와 같다.

```text
cmd_processor.h
  - CmdRequest, CmdResponse, CmdProcessorContext, CmdProcessor, status enum 정의
  - cmd_processor_* public wrapper API 선언

cmd_processor.c
  - status 문자열 변환
  - CmdRequest acquire/release 공통 helper
  - CmdResponse release 공통 helper
  - request setter helper
  - frontend validation 오류를 위한 CmdResponse 생성 helper
  - processor function pointer null check와 wrapper 구현

mock_cmd_processor.h / mock_cmd_processor.c
  - DB 구현 전 외부 진입점 테스트를 위한 임시 CmdProcessor
  - 테스트용 CmdProcessorContext와 processor-owned request/response buffer pool 생성
  - 실제 parser/executor와 연결하지 않음
```

`mock_cmd_processor`는 외부 진입점 구현체 검증용이다. 실제 DB 구현체가 아니며, 기존 SQL parser/executor에 연결하지 않는다.

## 8. 테스트 계획

이번 단계 테스트는 DB 정확성이 아니라 요청 처리 API 계약을 검증한다.

테스트할 항목:

- `ping` 요청이 정상 응답을 반환한다.
- `sql` 요청이 `CmdRequest`로 변환되어 mock processor에 전달된다.
- 응답의 `request_id`가 요청의 `request_id`와 일치한다.
- `CmdProcessorContext.max_sql_len`을 기준으로 요청 길이를 검증할 수 있다.
- 외부 진입점이 stack/heap에 직접 만든 request가 아니라 `cmd_processor_acquire_request()`로 얻은 request만 처리한다.
- `cmd_processor_set_sql_request()`가 SQL 문자열을 processor 소유 버퍼로 복사한다.
- request setter 실패 시 `cmd_processor_make_error_response()`로 processor-owned 오류 응답을 만들 수 있다.
- mock processor가 `BUSY`, `TIMEOUT`, `PROCESSING_ERROR`를 반환하면 호출자가 이를 그대로 사용할 수 있다.
- 여러 호출자가 동시에 `CmdProcessor.process()`를 호출할 수 있다.
- `body_format`과 `body_len`이 응답 payload를 정확히 설명한다.
- `release_request()`와 `release_response()`를 반복 호출하지 않는 정상 경로에서 processor-owned buffer slot이 재사용 가능 상태가 된다.

기존 DB 동작 테스트는 이번 단계의 필수 검증 대상이 아니다. 기존 코드를 수정하지 않기 때문이다.

## 9. 최종 결정

이번 단계의 최종 계약은 아래와 같다.

```text
기존 main.c는 수정하지 않는다.
기존 parser/executor/lexer/bptree는 수정하지 않는다.
현재 SQL parser/executor를 CmdProcessor에 연결하지 않는다.
이번 단계는 CmdProcessor API를 준비한다.
외부 진입점 구현체는 CmdProcessor만 호출한다.
CmdProcessor.process()는 동시 호출될 수 있는 API로 설계한다.
CmdProcessorContext는 processor 인스턴스 단위의 공용 context다.
CmdRequest는 요청 1개 단위의 요청별 context이며 CmdProcessor가 소유한다.
외부 진입점은 CmdRequest/CmdResponse와 내부 버퍼를 직접 할당하거나 해제하지 않는다.
v1에서는 session 개념을 의도적으로 제외한다.
동시 호출을 싱글스레드로 처리할지 멀티스레드로 처리할지는 DB 구현체의 책임이다.
동시 호출 중에도 data race/crash/response 오염이 발생하면 안 된다.
실제 DB 연결은 추후 DB 구현 변경 작업에서 수행한다.
```
