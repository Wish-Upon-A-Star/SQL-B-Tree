# REPLCmdProcessor 구현 계획

## 1. 목적

`REPLCmdProcessor`는 CLI에서 REPL처럼 SQL을 한 문장씩 받아 실행하기 위한 `CmdProcessor` 구현체다.

이 문서의 목표는 `CmdProcessor` 계약에 맞는 REPL 방식 구현체를 어떻게 만들지 정리하는 것이다. 현재 `main.c`와의 연결은 아직 하지 않는다. 즉, `--repl` 옵션, 기존 파일 실행 모드 전환, TCP 서버 연결은 이 문서의 구현 범위 밖이다.

```text
CLI REPL loop
        -> CmdRequest 생성
        -> REPLCmdProcessor.submit()
        -> SQL eval
        -> CmdResponse callback
        -> CLI 출력
```

핵심은 CLI가 직접 parser/executor를 호출하지 않고, `CmdProcessor` API를 통해 SQL 실행을 요청하게 만드는 것이다.

## 2. v1 범위

v1에서 만든다:

- `REPLCmdProcessor` 구현체.
- `repl_cmd_processor_create()` 생성 함수.
- processor-owned `CmdRequest`, `CmdResponse` pool.
- `CMD_REQUEST_PING` 처리.
- `CMD_REQUEST_SQL` 처리.
- SQL 실행 결과를 `CMD_BODY_TEXT` 응답으로 반환하는 흐름.
- 단위 테스트.

v1에서 하지 않는다:

- `main.c`에 REPL 모드 연결.
- TCP 서버에 기본 processor로 연결.
- SQL 실행 결과를 row 배열 JSON으로 구조화.
- SQL 실행 병렬화.
- transaction, cancellation, timeout 정책 추가.

## 3. 현재 구조에서 풀어야 할 결합

현재 SQL 실행 흐름은 `main.c`와 `executor.c`에 나뉘어 있다.

```text
main.c
        -> execute_sql_file()
        -> flush_sql_buffer()
        -> execute_sql_text()
        -> parse_statement()
        -> execute_insert/select/update/delete()
        -> stdout 출력
```

문제는 `execute_sql_text()`가 `main.c`의 `static` 함수이고, 실행 결과가 대부분 `printf()`로 stdout에 직접 쓰인다는 점이다. `REPLCmdProcessor`는 `CmdResponse`를 만들어야 하므로 실행 결과를 문자열로 받을 수 있어야 한다.

따라서 구현 단계에서는 SQL 실행 공통 모듈을 먼저 분리한다.

```text
SQL text
        -> reusable SQL executor facade
        -> output sink
        -> REPLCmdProcessor CmdResponse
```

## 4. 새 모듈 계획

### 4.1 SQL 실행 facade

새 모듈을 추가한다.

```text
cmd_processor/sql_repl_engine.h
cmd_processor/sql_repl_engine.c
```

이 모듈은 `main.c`의 SQL 한 문장 실행 로직을 재사용 가능한 형태로 옮긴다.

공개 API 초안:

```c
typedef struct {
    char *data;
    size_t len;
    size_t capacity;
    int truncated;
} SqlOutputBuffer;

typedef struct {
    CmdStatusCode status;
    int ok;
    int row_count;
    int affected_count;
} SqlEvalResult;

void sql_output_buffer_init(SqlOutputBuffer *buffer,
                            char *data,
                            size_t capacity);

SqlEvalResult sql_repl_eval(const char *sql,
                            SqlOutputBuffer *output);
```

`sql_repl_eval()`의 책임:

- UTF-8 BOM과 앞쪽 공백을 제거한다.
- 빈 SQL이면 `CMD_STATUS_OK`로 처리하고 body는 비운다.
- SQL은 `parse_statement()` 후 `executor_execute_statement()`로 넘긴다.
- parse 실패는 `CMD_STATUS_PARSE_ERROR`로 반환한다.
- 실행 실패를 명확히 감지할 수 있는 경로는 `CMD_STATUS_PROCESSING_ERROR`로 반환한다.
- 출력 문자열은 `SqlOutputBuffer`에 쌓는다.

주의: v1에서는 executor 전체를 구조화된 결과 모델로 바꾸지 않는다. 기존 실행 경로를 보존하되, REPL 응답 body는 실행 요약 text로 만든다.

### 4.2 Executor 실행 결과 사용

현재 병합된 실행 계층은 `executor_execute_statement()`를 제공한다. 이 함수는 `Statement`를 실행하고 `matched_rows`, `affected_rows`, `generated_id`를 반환한다.

v1에서는 stdout redirection이나 executor 전역 출력 sink를 추가하지 않는다. 대신 `sql_repl_eval()`이 `executor_execute_statement()`의 구조화된 실행 결과를 받아 사람이 읽을 수 있는 text body를 만든다.

```text
SELECT -> "SELECT matched_rows=N"
INSERT -> "INSERT affected_rows=N id=K"
UPDATE -> "UPDATE affected_rows=N"
DELETE -> "DELETE affected_rows=N"
```

이 방식은 기존 executor 출력 전체를 캡처하지는 않는다. SELECT row 배열이나 전체 CLI 출력 복원은 후속 개선으로 둔다.

## 5. REPLCmdProcessor 구조

새 파일을 추가한다.

```text
cmd_processor/repl_cmd_processor.h
cmd_processor/repl_cmd_processor.c
cmd_processor/repl_cmd_processor_test.c
```

공개 생성 함수:

```c
int repl_cmd_processor_create(const CmdProcessorContext *base_context,
                              CmdProcessor **out_processor);
```

기본 context:

| 필드 | 기본값 |
| --- | --- |
| `name` | `"repl_cmd_processor"` |
| `max_sql_len` | `MAX_SQL_LEN - 1` |
| `request_buffer_count` | `1` |
| `response_body_capacity` | `8192` |
| `shared_state` | `REPLCmdProcessorState *` |

`base_context` 값이 있으면 `name`, `max_sql_len`, `request_buffer_count`, `response_body_capacity`는 override할 수 있다.

내부 상태:

```c
typedef struct {
    CmdProcessor processor;
    CmdProcessorContext context;
    pthread_mutex_t mutex;
    REPLRequestSlot *request_slots;
    REPLResponseSlot *response_slots;
    size_t request_count;
    size_t response_count;
    size_t max_sql_len;
    size_t response_capacity;
} REPLCmdProcessorState;
```

request/response slot은 `mock_cmd_processor`와 같은 방식으로 processor가 소유한다. 외부 진입점은 wrapper API로 acquire/release만 한다.

## 6. 요청 처리 흐름

### 6.1 acquire_request

```text
repl_acquire_request()
        -> 빈 request slot 찾기
        -> request.sql buffer 연결
        -> in_use = 1
        -> CmdRequest* 반환
```

slot이 없으면 `-1`을 반환한다. 이 경우 외부 진입점은 `submit()`으로 진행하지 않는다.

### 6.2 submit

v1의 `submit()`은 동기 즉시 실행이다.

```text
repl_submit(processor, request, callback, user_data)
        -> request가 이 processor 소유인지 확인
        -> response slot 확보
        -> processor mutex 획득
        -> request type 평가
        -> CmdResponse 채움
        -> processor mutex 해제
        -> callback(processor, request, response, user_data)
        -> return 0
```

callback은 `submit()` 안에서 바로 호출된다. 그래도 외부 계약은 기존 `CmdProcessor`와 동일하다. 외부 진입점은 callback에서 request/response를 release해야 한다.

### 6.3 ping

`CMD_REQUEST_PING`은 DB를 건드리지 않는다.

응답:

| 필드 | 값 |
| --- | --- |
| `status` | `CMD_STATUS_OK` |
| `ok` | `1` |
| `body_format` | `CMD_BODY_TEXT` |
| `body` | `"pong"` |

### 6.4 sql

`CMD_REQUEST_SQL`은 `sql_repl_eval()`에 위임한다.

응답:

| 경우 | status | body |
| --- | --- | --- |
| 정상 실행 | `CMD_STATUS_OK` | 실행 요약 text |
| 빈 SQL | `CMD_STATUS_OK` | 빈 text |
| parse 실패 | `CMD_STATUS_PARSE_ERROR` | 오류 text |
| 실행 실패 | `CMD_STATUS_PROCESSING_ERROR` | 오류 text |
| body capacity 초과 | `CMD_STATUS_PROCESSING_ERROR` | `"response body capacity exceeded"` |

`row_count`, `affected_count`는 v1에서 확정하지 않는다. executor가 구조화된 결과를 반환하도록 바꾸는 것은 후속 개선으로 둔다.

## 7. REPL 관점의 CLI 사용 흐름

`main.c` 연결은 v1 범위 밖이지만, 구현체가 의도하는 사용 방식은 아래와 같다.

```text
REPL 시작
        -> repl_cmd_processor_create()
        -> loop
            -> prompt 출력
            -> 사용자 입력을 ; 기준으로 SQL 한 문장으로 조립
            -> CmdRequest acquire
            -> cmd_processor_set_sql_request()
            -> cmd_processor_submit(cli_callback)
            -> callback에서 body 출력
            -> request/response release
        -> cmd_processor_shutdown()
```

CLI loop는 `REPLCmdProcessor` 바깥에 둔다. `REPLCmdProcessor`는 prompt, stdin read, command history, `.exit` 같은 REPL UI 기능을 모른다.

## 8. 동시성 정책

v1은 단일 DB 상태를 보호하기 위해 SQL eval 구간을 직렬화한다.

- 여러 thread가 동시에 `submit()`을 호출해도 processor mutex로 하나씩 실행한다.
- request/response slot 관리도 같은 mutex로 보호한다.
- callback 호출 전에는 SQL eval lock을 해제한다.
- callback이 다시 processor API를 호출할 수 있으므로 lock을 잡은 상태로 callback을 호출하지 않는다.

이 정책은 TCP 계층과 함께 써도 안전하다. 다만 v1에서는 실제 SQL 처리량 병렬화를 목표로 하지 않는다.

## 9. 오류와 소유권 규칙

`submit()`이 `-1`을 반환하는 경우:

- processor/context/request/callback이 NULL이다.
- request가 이 processor에서 acquire한 slot이 아니다.
- response slot을 확보하지 못했다.

callback으로 전달하는 실패 응답:

- SQL parse 실패.
- SQL 실행 중 감지 가능한 처리 실패.
- 응답 body buffer 초과.
- 지원하지 않는 request type.

release 규칙:

- `CmdRequest`는 callback에서 `cmd_processor_release_request()`로 반환한다.
- `CmdResponse`는 callback에서 `cmd_processor_release_response()`로 반환한다.
- `cmd_processor_shutdown()`은 vtable의 REPL shutdown 구현을 호출하고, 남은 slot buffer와 mutex를 해제한다.
- shutdown은 active callback이 없는 시점에 호출하는 것을 전제로 한다.

## 10. 테스트 계획

새 테스트 target을 추가한다.

```text
make test-repl-cmd-processor
```

테스트 항목:

- 기본 context로 processor 생성 성공.
- base context override 적용.
- `ping` 요청 submit 후 callback 호출, request id 보존, body `"pong"` 확인.
- 정상 SQL 요청 submit 후 `CMD_STATUS_OK`, `CMD_BODY_TEXT`, request id 보존 확인.
- 잘못된 SQL 요청 submit 후 `CMD_STATUS_PARSE_ERROR` 확인.
- 너무 긴 SQL은 setter에서 `CMD_STATUS_SQL_TOO_LONG` 반환.
- response body capacity 초과 시 `CMD_STATUS_PROCESSING_ERROR` 확인.
- request/response release 후 slot 재사용 가능.
- foreign request submit은 실패하고 callback이 호출되지 않음.
- 동시에 여러 submit을 호출해도 callback 응답이 각 request id를 보존함.

테스트 SQL은 독립적인 임시 table 이름을 사용한다. 테스트가 만든 `.csv`, `.delta`, `.idx` 파일은 테스트 종료 시 정리한다.

## 11. 구현 순서

1. `cmd_processor/sql_repl_engine.h/.c`를 추가하고 `main.c` 연결 없이 SQL 한 문장 실행 facade를 둔다.
2. `sql_repl_eval()`에서 `parse_statement()`와 `executor_execute_statement()`를 호출해 text body를 만든다.
3. `repl_cmd_processor.h/.c`를 추가한다.
4. `repl_cmd_processor_test.c`와 Makefile target을 추가한다.
5. 기존 `cmd_processor` 테스트와 `tcp_cmd_processor` 테스트가 계속 통과하는지 확인한다.
6. 문서와 README 연결은 후속 단계에서 정리한다.

## 12. 최종 결정

```text
REPLCmdProcessor는 CmdProcessor 계약을 따르는 CLI REPL용 DB 실행 구현체다.
v1 submit은 동기 즉시 실행이다.
CLI loop와 main.c 연결은 REPLCmdProcessor 밖에 둔다.
응답 body는 CMD_BODY_TEXT다.
SQL 실행 결과를 얻기 위해 executor_execute_statement()를 사용한다.
DB 실행은 processor mutex로 직렬화한다.
구조화 JSON row 응답과 병렬 SQL 실행은 후속 개선으로 둔다.
```
