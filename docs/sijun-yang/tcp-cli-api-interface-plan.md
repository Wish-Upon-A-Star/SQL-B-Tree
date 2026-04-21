# TCP/CLI 공용 API 인터페이스 설계 계획

## 1. 목적

현재 프로젝트는 CLI 기반 SQL 처리기다. 다만 이번 단계에서는 기존 DB 코드와 실행 흐름을 수정하지 않는다.

이번 단계의 목표는 DB 구현을 먼저 바꾸지 않고, 나중에 CLI REPL, TCP, 기타 외부 API가 공통으로 사용할 수 있는 통신/API 인터페이스를 정의하고 TCP 구현체의 뼈대를 준비하는 것이다.

목표 구조는 아래와 같다.

```text
CLI REPL / TCP / 기타 외부 API
        -> 통신 인터페이스 API
        -> DbEngine API
        -> DB 구현체
```

중요한 점은 현재 단계에서 마지막 `DB 구현체` 연결을 수행하지 않는다는 것이다. 현재 SQL parser/executor를 `DbEngine`에 연결하는 작업은 추후 DB 구현 변경 작업에서 함께 진행한다.

## 2. 이번 단계의 범위

이번 단계에서 하는 일:

- `DbEngine` API 계약을 설계한다.
- 요청/응답 구조체를 정의한다.
- TCP JSON Lines 요청/응답 프로토콜을 정의한다.
- TCP 구현체가 어떤 방식으로 `DbEngine`을 호출할지 인터페이스 수준에서 준비한다.
- 동시 호출 가능성을 `DbEngine` API 요구사항으로 명시한다.

이번 단계에서 하지 않는 일:

- 기존 `main.c` 수정.
- 기존 `parser.c`, `executor.c`, `lexer.c`, `bptree.c` 수정.
- 기존 SQL 실행 흐름 분리.
- 현재 SQL parser/executor를 `DbEngine`에 연결.
- 기존 CLI 파일 실행 방식을 새 API로 교체.
- DB 내부 동시성 구현.

즉, 이번 단계의 산출물은 기존 코드에 연결되지 않은 독립 API/통신 계층이다.

## 3. 핵심 설계 방향

첫 공개 요청 형식은 SQL 문자열 중심으로 둔다.

이유는 다음과 같다.

- 나중에 현재 SQL parser/executor와 연결하기 쉽다.
- TCP, CLI REPL, 테스트 클라이언트가 같은 요청 모델을 쓸 수 있다.
- DB 내부 구현이 나중에 구조화 CRUD API로 바뀌더라도 외부 프로토콜을 유지할 수 있다.

다만 `DbEngine`은 SQL 문자열만 처리하는 전용 인터페이스로 고정하지 않는다. `PING`, `CLOSE`, 추후 structured request 같은 확장을 받을 수 있도록 요청 타입을 둔다.

## 4. DbEngine API 계약

공용 API 헤더는 예를 들어 `db_api.h`로 둔다.

권장 요청 모델:

```c
typedef enum {
    DB_REQUEST_SQL,
    DB_REQUEST_PING,
    DB_REQUEST_CLOSE
} DbRequestType;

typedef enum {
    DB_STATUS_OK,
    DB_STATUS_BAD_REQUEST,
    DB_STATUS_SQL_TOO_LONG,
    DB_STATUS_PARSE_ERROR,
    DB_STATUS_ENGINE_ERROR,
    DB_STATUS_BUSY,
    DB_STATUS_TIMEOUT,
    DB_STATUS_INTERNAL_ERROR
} DbStatusCode;

typedef struct {
    char request_id[64];
    char session_id[64];
    DbRequestType type;
    const char *sql;
    size_t sql_len;
    int emit_traces;
    int readonly;
    int timeout_ms;
} DbRequest;

typedef struct {
    char request_id[64];
    DbStatusCode status;
    int ok;
    int row_count;
    int affected_count;
    char *body;
    char *error_message;
    long elapsed_us;
} DbResponse;

typedef struct DbEngine {
    void *ctx;
    int (*execute)(void *ctx, const DbRequest *request, DbResponse *response);
    void (*shutdown)(void *ctx);
} DbEngine;
```

`DbEngine.execute()`의 책임:

- `DbRequest`를 받아 처리한다.
- 성공/실패를 `DbResponse`로 반환한다.
- 호출자가 넘긴 `request_id`를 응답에 보존한다.
- 내부 처리 방식은 구현체가 결정한다.

## 5. 동시성 요구사항

동시성은 TCP frontend의 단순 구현 세부사항이 아니라 `DbEngine` API의 요구사항이다.

요구사항:

- `DbEngine.execute()`는 여러 호출 지점에서 동시에 호출될 수 있다고 가정한다.
- CLI REPL은 보통 단일 호출 흐름이지만, TCP나 외부 API 구현체는 여러 요청을 동시에 호출할 수 있다.
- 이 동시 호출을 실제로 싱글스레드 큐로 처리할지, mutex로 보호할지, worker pool로 병렬 처리할지, read/write lock으로 분리할지는 DB 구현체의 책임이다.
- 통신 계층은 특정 DB 동시성 모델을 강제하지 않는다.

따라서 API 문서에는 다음 계약을 명시한다.

```text
DbEngine 구현체는 execute()의 동시 호출 가능성을 고려해야 한다.
구현체는 자신의 동시성 정책을 내부적으로 보장하거나, 명시적으로 BUSY/TIMEOUT 같은 상태를 반환해야 한다.
TCP 구현체는 요청을 받을 수 있지만, DB 실행 직렬화 여부를 자체적으로 결정하지 않는다.
```

## 6. TCP 프로토콜

TCP 프로토콜은 JSON Lines로 둔다.

규칙:

- 요청 1개는 JSON 객체 1줄이다.
- 응답 1개는 JSON 객체 1줄이다.
- TCP 연결 하나에서 여러 요청을 순서대로 보낼 수 있다.
- 여러 TCP 클라이언트가 동시에 접속할 수 있다.
- 각 요청은 `DbRequest`로 변환되어 `DbEngine.execute()`에 전달된다.

기본 SQL 요청:

```json
{"id":"1","op":"sql","sql":"SELECT * FROM users WHERE id = 1;"}
```

기본 성공 응답:

```json
{"id":"1","ok":true,"status":"OK","body":"...","elapsed_us":1234}
```

기본 실패 응답:

```json
{"id":"1","ok":false,"status":"PARSE_ERROR","error":"invalid SQL statement","elapsed_us":321}
```

권장 요청 필드:

| 필드 | 필수 여부 | 의미 |
|---|---:|---|
| `id` | 필수 | 클라이언트 요청 ID. 응답에서 그대로 반환한다. |
| `op` | 필수 | `sql`, `ping`, `close` 중 하나. |
| `sql` | `op=sql`일 때 필수 | 실행할 SQL 문자열. |
| `session` | 선택 | 추후 session/transaction 상태를 위한 ID. |
| `timeout_ms` | 선택 | 실행 timeout 힌트. |
| `readonly` | 선택 | SELECT 전용 요청 같은 최적화를 위한 힌트. |
| `trace` | 선택 | index/scan trace 포함 여부. |

권장 응답 필드:

| 필드 | 의미 |
|---|---|
| `id` | 원래 요청 ID. |
| `ok` | 성공 여부. |
| `status` | 안정적인 상태 문자열. |
| `body` | 실행 결과 텍스트 또는 직렬화된 결과. |
| `error` | 실패 시 오류 메시지. |
| `row_count` | SELECT 계열 결과 수. 구현체가 모르면 생략 가능. |
| `affected_count` | INSERT/UPDATE/DELETE 영향 행 수. 구현체가 모르면 생략 가능. |
| `elapsed_us` | 처리 시간. 단위는 microsecond. |

## 7. TCP 구현체 책임

TCP 구현체는 DB 구현을 알지 못해야 한다. TCP 구현체가 아는 것은 `DbEngine`뿐이다.

TCP 구현체의 책임:

- socket listen/bind/accept 처리.
- 클라이언트 연결별 read/write 처리.
- JSON Lines 요청 파싱.
- 요청 유효성 검증.
- JSON 요청을 `DbRequest`로 변환.
- `DbEngine.execute()` 호출.
- `DbResponse`를 JSON Lines 응답으로 변환.
- 연결 종료 요청 처리.

TCP 구현체가 하지 않는 일:

- SQL 파싱.
- DB 파일 접근.
- B+ Tree 접근.
- 현재 `executor.c` 호출.
- DB 요청 직렬화 정책 강제.
- DB 내부 lock 관리.

## 8. CLI/REPL과의 관계

CLI REPL도 나중에는 같은 `DbEngine` API를 호출할 수 있다.

다만 이번 단계에서는 `main.c`를 수정하지 않으므로 기존 CLI 실행 흐름은 그대로 둔다. REPL 구현 또는 기존 CLI와의 연결은 추후 작업으로 분리한다.

향후 REPL이 추가되면 권장 동작은 아래와 같다.

- quote 밖의 `;`가 나올 때까지 줄을 계속 읽는다.
- 완성된 SQL 문자열을 `DbRequest`로 만든다.
- `DbEngine.execute()`를 호출한다.
- `DbResponse.body`를 출력한다.
- `.exit`, `.quit`, `exit`, EOF에서 종료한다.

## 9. 파일/모듈 계획

이번 단계에서 추가될 수 있는 파일 예시는 아래와 같다.

```text
db_api.h
  - DbRequest, DbResponse, DbEngine, status enum 정의

db_api.c
  - status 문자열 변환
  - DbResponse 초기화/정리 helper

tcp_frontend.h
  - TCP server 설정 구조체
  - TCP server 시작/종료 함수 선언

tcp_frontend.c
  - TCP JSON Lines server 구현
  - JSON 요청을 DbRequest로 변환
  - DbResponse를 JSON 응답으로 변환

mock_db_engine.h / mock_db_engine.c
  - DB 구현 전 TCP 테스트를 위한 임시 DbEngine
  - 실제 parser/executor와 연결하지 않음
```

`mock_db_engine`은 TCP 구현체 검증용이다. 실제 DB 구현체가 아니며, 기존 SQL parser/executor에 연결하지 않는다.

## 10. 구현 순서

권장 구현 순서:

1. `db_api.h`에 `DbRequest`, `DbResponse`, `DbEngine` 계약을 정의한다.
2. `db_api.c`에 response 초기화/정리, status 문자열 변환 helper를 만든다.
3. `tcp_frontend.h/c`에 JSON Lines TCP server를 만든다.
4. `mock_db_engine`으로 TCP 요청/응답을 검증한다.
5. 동시 TCP 연결에서 `DbEngine.execute()`가 동시에 호출될 수 있는 테스트를 만든다.
6. 실제 SQL parser/executor 연결은 하지 않는다.
7. `main.c` 연결도 하지 않는다.

## 11. 테스트 계획

이번 단계 테스트는 DB 정확성이 아니라 API/통신 계층을 검증한다.

테스트할 항목:

- `ping` 요청이 정상 응답을 반환한다.
- `sql` 요청이 `DbRequest`로 변환되어 mock engine에 전달된다.
- 응답의 `id`가 요청의 `id`와 일치한다.
- 잘못된 JSON은 `BAD_REQUEST`로 응답한다.
- `op=sql`인데 `sql`이 없으면 `BAD_REQUEST`로 응답한다.
- SQL 길이가 제한을 넘으면 `SQL_TOO_LONG`으로 응답한다.
- 여러 클라이언트가 동시에 접속할 수 있다.
- 여러 요청이 동시에 `DbEngine.execute()`까지 도달할 수 있다.
- mock engine이 `BUSY`, `TIMEOUT`, `ENGINE_ERROR`를 반환하면 TCP 응답에 그대로 반영된다.

기존 DB 동작 테스트는 이번 단계의 필수 검증 대상이 아니다. 기존 코드를 수정하지 않기 때문이다.

## 12. 최종 결정

이번 단계의 최종 계약은 아래와 같다.

```text
기존 main.c는 수정하지 않는다.
기존 parser/executor/lexer/bptree는 수정하지 않는다.
현재 SQL parser/executor를 DbEngine에 연결하지 않는다.
이번 단계는 DbEngine API와 TCP JSON Lines 구현체만 준비한다.
TCP 구현체는 DbEngine만 호출한다.
DbEngine.execute()는 동시 호출될 수 있는 API로 설계한다.
동시 호출을 싱글스레드로 처리할지 멀티스레드로 처리할지는 DB 구현체의 책임이다.
실제 DB 연결은 추후 DB 구현 변경 작업에서 수행한다.
```
