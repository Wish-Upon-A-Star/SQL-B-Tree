# TCP JSON Lines 인터페이스 설계 계획

## 1. 목적

이 문서는 TCP 기반 외부 API 구현 계획만 다룬다. TCP 구현체는 DB 구현을 알지 못하고, 공통 요청 처리 인터페이스인 `CmdProcessor`만 호출한다.

목표 구조는 아래와 같다.

```text
TCP client
        -> TCP JSON Lines frontend
        -> CmdProcessor.process()
        -> DB 구현체
```

현재 단계에서는 기존 `main.c`, SQL parser, executor에 연결하지 않는다.

## 2. 이번 단계의 범위

이번 단계에서 하는 일:

- TCP JSON Lines 요청/응답 프로토콜을 정의한다.
- TCP 구현체가 `CmdProcessor`를 호출하는 방식을 정의한다.
- DB 내부 구현과 별개로 여러 TCP 클라이언트의 요청을 동시에 받고 응답할 수 있는 구조를 설계한다.
- mock `CmdProcessor`로 TCP 계층을 검증할 수 있게 한다.

이번 단계에서 하지 않는 일:

- 기존 `main.c` 수정.
- 기존 SQL parser/executor 호출.
- DB 요청 직렬화 정책 강제.
- DB 내부 lock 또는 worker queue 구현.

## 3. TCP 프로토콜

TCP 프로토콜은 JSON Lines로 둔다.

규칙:

- 요청 1개는 JSON 객체 1줄이다.
- 응답 1개는 JSON 객체 1줄이다.
- TCP 연결 하나에서 여러 요청을 순서대로 보낼 수 있다.
- 여러 TCP 클라이언트가 동시에 접속할 수 있다.
- 각 요청은 `CmdRequest`로 변환되어 `CmdProcessor.process()`에 전달된다.

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
| `id` | 필수 | 클라이언트 요청 ID. `CmdRequest.request_id`로 변환한다. |
| `op` | 필수 | `sql`, `ping`, `close` 중 하나. `close`는 TCP frontend가 직접 처리하고 `CmdProcessor`로 전달하지 않는다. |
| `sql` | `op=sql`일 때 필수 | 실행할 SQL 문자열. |

session 제외 규칙:

- v1 TCP 요청에는 `session` 필드를 두지 않는다.
- TCP socket connection은 TCP frontend의 lifecycle이며 `CmdProcessor` 요청 context가 아니다.
- 같은 socket에서 여러 요청을 보내도 각 요청은 독립적인 `CmdRequest`로 처리된다.
- `op=close`는 현재 TCP connection 종료 요청이다. processor shutdown이나 DB shutdown이 아니다.

권장 응답 필드:

| 필드 | 의미 |
|---|---|
| `id` | 원래 요청 ID. |
| `ok` | 성공 여부. |
| `status` | `CmdStatusCode`에서 온 안정적인 상태 문자열. 요청 실패의 주된 문맥은 이 필드로 판단한다. |
| `body` | `CmdResponse.body_format`에 따라 직렬화한 실행 결과. |
| `error` | 실패 시 사람이 읽을 수 있는 오류 메시지. 상태 판정 기준은 아니다. |
| `row_count` | 구현체가 제공하면 포함한다. |
| `affected_count` | 구현체가 제공하면 포함한다. |
| `elapsed_us` | 처리 시간. 단위는 microsecond. |

`CmdResponse.body_format` 변환 규칙:

- `CMD_BODY_NONE`: `body` 필드를 생략하거나 `null`로 둔다.
- `CMD_BODY_TEXT`: `body`를 JSON string으로 escape해서 넣는다.
- `CMD_BODY_JSON`: `body`를 JSON value로 넣는다. 단, TCP 구현체가 JSON 유효성을 확인하지 못하면 안전하게 JSON string으로 escape한다.
- `body_len`을 기준으로 payload 길이를 처리하고, NUL 종료 여부에 의존하지 않는다.

## 4. TCP 구현체 책임

TCP 구현체의 책임:

- socket listen/bind/accept 처리.
- 클라이언트 연결별 read/write 처리.
- JSON Lines 요청 파싱.
- 요청 유효성 검증.
- `cmd_processor_acquire_request()`로 processor-owned `CmdRequest` 확보.
- JSON 요청 필드를 `CmdProcessor` setter API로 복사.
- frontend validation 오류는 가능한 경우 `cmd_processor_make_error_response()`로 processor-owned `CmdResponse` 생성.
- `CmdProcessor.process()` 호출.
- `CmdResponse`를 JSON Lines 응답으로 변환.
- 요청/응답 처리가 끝난 뒤 `CmdProcessor.release_request()`와 `CmdProcessor.release_response()` 호출.
- 연결 종료 요청 처리.

TCP 구현체가 하지 않는 일:

- SQL 파싱.
- DB 파일 접근.
- B+ Tree 접근.
- 현재 `executor.c` 호출.
- DB 요청 직렬화 정책 강제.
- DB 내부 lock 관리.

## 5. TCP Server API

TCP frontend는 `CmdProcessor`를 외부에서 주입받아야 한다.

예상 인터페이스:

```c
typedef struct {
    const char *host;
    int port;
    int backlog;
    int max_clients;
    int worker_count;
    int job_queue_capacity;
    int read_timeout_ms;
    int write_timeout_ms;
    size_t max_line_bytes;
    int max_inflight_per_client;
    CmdProcessor *processor;
} TcpServerConfig;

typedef struct TcpServer TcpServer;

int tcp_server_start(const TcpServerConfig *config, TcpServer **out_server);
void tcp_server_stop(TcpServer *server);
```

규칙:

- `processor`는 필수다.
- TCP server는 `processor->context->max_sql_len`을 기준으로 SQL 길이를 검증한다.
- TCP server는 `processor->context->shared_state`를 해석하지 않는다.
- TCP server는 DB 구현체의 동시성 정책을 가정하지 않는다.

## 6. 동시성 정책

TCP 구현체는 DB 내부 구현과 별개로 여러 클라이언트 연결을 받을 수 있어야 한다. 성능 최적화보다 학습과 구현 난이도를 우선하므로, v1은 POSIX socket + pthread + blocking I/O 기반의 worker pool 구조로 둔다.

다만 DB 실행 동시성 정책은 `CmdProcessor` 구현체의 책임이다.

TCP 구현체 규칙:

- 여러 client connection을 동시에 유지할 수 있다.
- 여러 요청을 받아 `CmdProcessor.process()`까지 전달할 수 있다.
- worker 개수는 `TcpServerConfig.worker_count`로 조절한다.
- 요청 queue 크기는 `TcpServerConfig.job_queue_capacity`로 조절한다.
- socket, job queue, client 목록 같은 TCP 계층 자원만 TCP 계층에서 보호한다.
- DB 실행 직렬화, DB lock, DB worker queue는 TCP 계층에서 강제하지 않는다.
- `CmdProcessor.process()`가 `BUSY`, `TIMEOUT`, `PROCESSING_ERROR`를 반환하면 그 상태를 JSON 응답으로 그대로 변환한다.

권장 구현 방식은 acceptor + worker pool + job queue 방식이다.

```text
main/server thread
        -> socket()
        -> bind()
        -> listen()
        -> worker thread N개 생성

accept loop
        -> accept()
        -> client socket을 job queue에 enqueue

worker thread
        -> job queue에서 client socket dequeue
        -> 해당 socket read loop
        -> JSON line 파싱
        -> CmdProcessor에서 CmdRequest 확보
        -> JSON 필드를 CmdRequest 버퍼로 복사
        -> CmdProcessor.process()
        -> CmdResponse를 JSON line으로 직렬화
        -> 해당 socket write
        -> close 조건까지 반복 후 socket close
```

이 방식의 의미:

- accept loop는 연결 수락과 queue 삽입만 담당한다.
- worker 수만큼 client socket을 동시에 처리할 수 있다.
- worker thread는 blocking `recv()`/`send()`를 사용해도 된다.
- 한 worker가 한 client socket을 맡아 연결이 끝날 때까지 처리한다.
- worker 수보다 많은 client는 job queue에서 대기한다.
- 여러 worker가 동시에 `CmdProcessor.process()`를 호출할 수 있다.
- 이 동시 호출을 실제 병렬 처리할지, 내부 lock으로 직렬화할지, `BUSY`로 거절할지는 `CmdProcessor` 구현체 책임이다.
- 같은 연결 안에서는 v1 기준 요청 순서대로 응답한다.

이 구조는 epoll, kqueue, async I/O 같은 복잡한 최적화를 사용하지 않는다. 성능이 부족해지면 후속 개선에서 아래 방향을 검토한다.

- non-blocking socket + event loop.
- client별 read/write 상태 machine.
- 요청 단위 worker pool 분리.
- connection keep-alive와 request processing의 완전 분리.

### 6.1 요청/응답 순서

v1에서는 같은 TCP 연결 안에서 요청 순서와 응답 순서를 맞춘다.

규칙:

- 한 연결에서 여러 JSON line 요청을 연속으로 보낼 수 있다.
- 같은 연결의 응답은 요청을 읽은 순서대로 보낸다.
- 서로 다른 연결 사이의 처리 순서는 보장하지 않는다.
- 응답 식별은 항상 `id` 필드로 한다.

이 정책을 두면 클라이언트는 단순하게 구현할 수 있고, 여러 클라이언트 간 동시성은 유지할 수 있다.

### 6.2 Backpressure와 제한

TCP server는 DB 구현체와 무관하게 자기 자원을 보호해야 한다.

권장 제한:

- `max_clients`: 동시에 유지할 최대 client connection 수.
- `worker_count`: client socket을 처리할 worker thread 개수.
- `job_queue_capacity`: accept된 client socket이 worker를 기다릴 수 있는 queue 크기.
- `max_line_bytes`: JSON line 하나의 최대 크기. 기본값은 `processor->context->max_sql_len`보다 약간 크게 둔다.
- `read_timeout_ms`: 클라이언트가 한 줄을 너무 오래 완성하지 않을 때 연결 종료.
- `write_timeout_ms`: 응답을 너무 오래 받지 않는 클라이언트 연결 종료.
- `max_inflight_per_client`: 한 클라이언트가 동시에 처리 중으로 둘 수 있는 요청 수. v1 기본값은 `1`.

v1 기본값을 `max_inflight_per_client = 1`로 두면 같은 연결의 응답 순서를 쉽게 보장할 수 있다. 그래도 여러 클라이언트는 각자의 worker에서 동시에 처리된다.

### 6.3 오류 처리

TCP 계층에서 직접 판단하는 오류:

- JSON line이 너무 길면 `BAD_REQUEST` 또는 `SQL_TOO_LONG` 응답 후 연결 유지 또는 종료.
- JSON 파싱 실패는 `BAD_REQUEST`.
- `id` 누락은 `BAD_REQUEST`.
- `op` 누락 또는 알 수 없는 `op`는 `BAD_REQUEST`.
- `op=sql`에서 `sql` 누락은 `BAD_REQUEST`.
- `sql` 길이가 `processor->context->max_sql_len`을 넘으면 `SQL_TOO_LONG`.
- 위 오류 응답은 가능한 경우 `cmd_processor_make_error_response()`로 만들고, 응답 직렬화 후 `CmdProcessor.release_response()`로 해제한다.

`CmdProcessor.process()` 이후의 처리 오류:

- processor가 반환한 status를 JSON 응답에 그대로 반영한다.
- processor가 응답 body/error를 비워도 TCP 계층은 최소 JSON 응답을 생성한다.
- processor 호출 중 TCP 연결이 끊기면 TCP 계층은 응답 전송만 포기한다. 이미 시작된 processor 처리를 취소할지는 processor 구현체 책임이다.
- `process()`가 0이 아닌 값을 반환하면 TCP 계층은 `INTERNAL_ERROR` 응답을 생성하거나 연결을 종료한다. 일반적인 요청 실패는 `process()==0`과 `CmdResponse.status`로 표현되어야 한다.

### 6.4 Thread Safety 경계

TCP 구현체가 보장할 것:

- client socket별 read/write 상태는 해당 socket을 dequeue한 worker가 소유한다.
- job queue, client 수, 종료 플래그 같은 공유 상태는 mutex/condition variable로 보호한다.
- `CmdRequest`와 `CmdResponse`는 `CmdProcessor`가 소유한다.
- TCP 계층은 `CmdRequest`를 직접 stack/heap에 만들지 않고 `cmd_processor_acquire_request()`로 얻는다.
- TCP 계층은 JSON parser 내부 문자열 포인터를 `CmdRequest.sql`에 직접 넣지 않고 setter API를 통해 processor-owned buffer로 복사한다.
- `CmdResponse`를 JSON으로 직렬화한 뒤에는 반드시 `CmdProcessor.release_response()`를 호출한다.
- 요청 처리가 끝나면 반드시 `CmdProcessor.release_request()`를 호출한다.

TCP 구현체가 보장하지 않는 것:

- DB 파일 상태 보호.
- SQL 실행 순서의 전역 직렬화.
- 여러 요청 간 transaction 격리.
- `processor->context->shared_state` 내부 thread safety.

이 경계 때문에 TCP 구현은 DB 내부 구현과 분리된 상태에서도 여러 클라이언트의 동시 송수신을 처리할 수 있다.

## 7. 파일/모듈 계획

이번 단계에서 추가될 수 있는 TCP 관련 파일 예시는 아래와 같다.

```text
tcp_frontend.h
  - TcpServerConfig
  - TcpServer opaque type
  - tcp_server_start / tcp_server_stop 선언

tcp_frontend.c
  - TCP JSON Lines server 구현
  - JSON 요청을 CmdRequest로 변환
  - CmdResponse를 JSON 응답으로 변환
  - accept loop, worker pool, job queue 관리
  - client별 read/write timeout과 연결 종료 처리
```

테스트에는 `000_cmd_processor_api.md`의 `mock_cmd_processor`를 사용한다.

## 8. 테스트 계획

TCP 계층 테스트 항목:

- 정상 `ping` 요청.
- 정상 `sql` 요청.
- 잘못된 JSON.
- `id` 누락.
- `op` 누락.
- `op=sql`에서 `sql` 누락.
- `CmdProcessorContext.max_sql_len`보다 긴 SQL.
- 여러 클라이언트의 동시 연결.
- 한 연결에서 여러 요청을 순서대로 전송.
- `worker_count`만큼 client socket이 동시에 처리되는지 확인.
- worker 수보다 많은 클라이언트가 접속하면 job queue에서 대기하는지 확인.
- `job_queue_capacity`를 넘으면 새 연결을 거절하거나 즉시 닫는지 확인.
- 여러 클라이언트에서 동시에 요청을 보내 여러 worker가 `CmdProcessor.process()`까지 도달하는지 확인.
- 같은 연결에서는 요청 순서대로 응답이 돌아오는지 확인.
- mock processor의 `BUSY`, `TIMEOUT`, `PROCESSING_ERROR` 응답 변환.

기존 DB 동작 테스트는 이번 TCP 문서의 검증 대상이 아니다.

## 9. 최종 결정

TCP 관련 최종 계약은 아래와 같다.

```text
TCP 구현체는 CmdProcessor만 호출한다.
TCP 구현체는 SQL parser/executor를 직접 호출하지 않는다.
TCP 구현체는 DB 동시성 정책을 강제하지 않는다.
TCP 요청은 JSON Lines로 받는다.
TCP 응답도 JSON Lines로 반환한다.
여러 TCP 클라이언트의 요청을 동시에 받고 응답할 수 있어야 한다.
v1 구현은 POSIX socket, pthread, blocking I/O, worker pool, job queue 기반으로 한다.
같은 연결에서는 v1 기준 요청 순서대로 응답한다.
서로 다른 연결 사이의 처리 순서는 보장하지 않는다.
실제 DB 연결은 추후 DB 구현 변경 작업에서 수행한다.
```
