# TCP JSON Lines 인터페이스 설계 및 구현 실행 계획

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
- 클라이언트와 서버의 책임을 구분한다.
- TCP connection과 socket fd의 소유권을 정리한다.
- TCP 구현체가 `CmdProcessor`를 호출하는 방식을 정의한다.
- DB 내부 구현과 별개로 여러 TCP client connection을 유지할 수 있는 구조를 설계한다.

이번 단계에서 하지 않는 일:

- 기존 `main.c` 수정.
- 기존 SQL parser/executor 호출.
- DB 내부 lock 관리.
- DB 내부 실행 구조 구현.
- DB 요청 병렬 실행 정책 결정.

## 3. 클라이언트와 서버

TCP 통신은 두 프로그램 사이의 연결이다.

- 클라이언트는 요청을 보내는 외부 프로그램이다.
- 서버는 요청을 받아 처리하고 응답을 보내는 TCP JSONL API 서버다.
- 이 프로젝트에서 클라이언트는 DBMS 기능을 사용하려는 프로그램이다.
- 이 프로젝트에서 서버는 `CmdProcessor`에 요청을 전달하는 frontend다.

```text
client process
        -> request JSON line
        -> TCP connection
        -> server process

server process
        -> response JSON line
        -> TCP connection
        -> client process
```

## 4. TCP 프로토콜

TCP 프로토콜은 JSON Lines로 둔다.

규칙:

- 요청 1개는 JSON 객체 1줄이다.
- 응답 1개는 JSON 객체 1줄이다.
- TCP connection 하나에서 여러 요청을 연속으로 보낼 수 있다.
- 클라이언트는 이전 요청의 응답을 기다리지 않고 다음 요청을 보낼 수 있다.
- 같은 connection 안의 응답 순서는 요청 순서와 다를 수 있다.
- 클라이언트는 응답의 `id`로 원래 요청을 매핑해야 한다.
- 각 요청은 `CmdRequest`로 변환되어 `CmdProcessor.process()`에 전달된다.

기본 SQL 요청:

```json
{"id":"1","op":"sql","sql":"SELECT * FROM users WHERE id = 1;"}
```

기본 성공 응답:

```json
{"id":"1","ok":true,"status":"OK","body":"..."}
```

기본 실패 응답:

```json
{"id":"1","ok":false,"status":"PARSE_ERROR","error":"invalid SQL statement"}
```

요청 필드:

| 필드 | 필수 여부 | 의미 |
|---|---:|---|
| `id` | 필수 | 클라이언트가 요청마다 생성하는 고유 ID. 응답 매핑에 사용한다. |
| `op` | 필수 | `sql`, `ping`, `close` 중 하나. |
| `sql` | `op=sql`일 때 필수 | 실행할 SQL 문자열. |

응답 필드:

| 필드 | 의미 |
|---|---|
| `id` | 원래 요청 ID. |
| `ok` | 성공 여부. |
| `status` | `CmdStatusCode`에서 온 안정적인 상태 문자열. |
| `body` | 실행 결과. |
| `error` | 실패 시 사람이 읽을 수 있는 오류 메시지. |
| `row_count` | 구현체가 제공하면 포함한다. |
| `affected_count` | 구현체가 제공하면 포함한다. |

## 5. 요청 ID와 응답 매핑

응답 순서를 보장하지 않기 때문에 요청 ID가 필수다.

클라이언트 책임:

- 요청마다 고유한 `id`를 생성한다.
- `id -> 요청 상태` 매핑을 유지한다.
- 응답 순서가 요청 순서와 달라도 `id`로 결과를 찾는다.
- 같은 connection에서 처리 중인 요청끼리는 `id`가 중복되지 않게 한다.

서버 책임:

- 요청의 `id`를 `CmdRequest.request_id`로 전달한다.
- 응답에 반드시 원래 요청의 `id`를 넣는다.
- 같은 connection의 in-flight 요청 ID 중복을 거절한다.
- 응답 순서를 보장하지 않는다.

## 6. Socket FD 소유권

TCP connection 하나에는 양쪽 프로세스가 각각 자기 socket fd를 가진다.

| FD | 개수 | 소유자 | 의미 |
|---|---:|---|---|
| `listen_fd` | 서버 인스턴스당 보통 1개 | 서버 | 새 TCP 연결을 받기 위한 socket. |
| server-side `client_fd` | client connection마다 1개 | 서버 | 특정 클라이언트와 통신하는 socket. |
| client-side socket fd | client connection마다 1개 | 클라이언트 | 서버와 통신하는 클라이언트 쪽 socket. |

규칙:

- 클라이언트의 socket fd와 서버의 `client_fd`는 같은 TCP connection의 양 끝이다.
- 두 fd는 서로 다른 프로세스에 있으므로 같은 값도 아니고 공유되는 값도 아니다.
- 서버의 `listen_fd`는 요청/응답 데이터를 주고받는 fd가 아니다.
- 서버는 `client_fd`에서 요청을 읽고 같은 `client_fd`로 응답을 쓴다.
- 하나의 `client_fd`에 여러 in-flight 요청이 걸릴 수 있다.
- 같은 `client_fd`에 여러 응답이 비동기적으로 쓰일 수 있으므로 서버는 write 충돌을 막아야 한다.

## 7. TCP 구현체 책임

TCP 구현체의 책임:

- socket listen/bind/accept 처리.
- client connection 상태 관리.
- connection별 JSON Lines read/write 처리.
- 요청 유효성 검증.
- connection별 in-flight 요청 ID 관리.
- `CmdProcessor` wrapper API 호출.
- `CmdResponse`를 JSON Lines 응답으로 변환.
- 같은 `client_fd`에 대한 write 보호.
- connection 종료 요청 처리.

TCP 구현체가 하지 않는 일:

- SQL 파싱.
- DB 파일 접근.
- B+ Tree 접근.
- 현재 `executor.c` 호출.
- DB 내부 lock 관리.
- DB 내부 실행 설정.
- DB 요청 병렬 실행 정책 결정.

## 8. TCP Server API

TCP frontend는 `CmdProcessor`를 외부에서 주입받아야 한다.

확정 인터페이스:

```c
typedef struct {
    const char *host;
    int port;
    int backlog;
    int max_clients;
    int read_timeout_ms;
    int write_timeout_ms;
    size_t max_line_bytes;
    int max_inflight_per_client;
    CmdProcessor *processor;
} TcpServerConfig;

typedef struct TcpServer TcpServer;

int tcp_server_start(const TcpServerConfig *config, TcpServer **out_server);
int tcp_server_get_port(const TcpServer *server);
void tcp_server_stop(TcpServer *server);
```

규칙:

- `processor`는 필수다.
- `processor->context`도 필수다.
- `host == NULL`이면 `"127.0.0.1"`로 본다.
- v1은 IPv4 `AF_INET`만 지원한다.
- `port == 0`이면 OS가 빈 포트를 배정하게 하고, 테스트는 `tcp_server_get_port()`로 실제 포트를 얻는다.
- `backlog <= 0`이면 `16`을 기본값으로 쓴다.
- `max_clients <= 0`이면 `128`을 기본값으로 쓴다.
- `max_line_bytes == 0`이면 `processor->context->max_sql_len + 1024`를 기본값으로 쓴다.
- `max_inflight_per_client <= 0`이면 `16`을 기본값으로 쓴다.
- TCP server는 `processor->context->shared_state`를 해석하지 않는다.

## 9. Connection 처리 모델

v1 TCP frontend는 사용자 또는 클라이언트 connection을 유지하고, 들어온 요청을 `CmdProcessor`로 전달한다.

```text
accept loop
        -> accept()
        -> TcpConnection 생성
        -> connection read loop 시작

connection read loop
        -> JSON line read
        -> id/op/sql 검증
        -> in-flight id 등록
        -> CmdProcessor.process()로 요청 전달
        -> 다음 JSON line read

response path
        -> CmdResponse 수신
        -> CmdResponse를 JSON line으로 변환
        -> 같은 client_fd에 response write
        -> in-flight id 제거
```

중요한 경계:

- TCP 계층은 connection과 request framing을 관리한다.
- TCP 계층은 DB 내부 처리 구조를 알지 못한다.
- TCP 계층은 DB 내부 병렬 처리 방식을 알지 못한다.
- DB 실행 동시성 정책은 `CmdProcessor` 구현체 책임이다.

## 10. Backpressure와 제한

TCP server는 DB 구현체와 무관하게 자기 자원을 보호해야 한다.

권장 제한:

- `max_clients`: 동시에 유지할 최대 client connection 수.
- `max_inflight_per_client`: 한 connection에서 동시에 처리 중으로 둘 수 있는 요청 수.
- `max_line_bytes`: JSON line 하나의 최대 크기.
- `read_timeout_ms`: 클라이언트가 한 줄을 너무 오래 완성하지 않을 때 연결 종료.
- `write_timeout_ms`: 응답을 너무 오래 받지 않는 클라이언트 연결 종료.

`max_inflight_per_client`를 넘으면 TCP 계층은 새 요청을 거절하거나 `BUSY` 또는 `BAD_REQUEST` 응답을 반환한다.

## 11. 오류 처리

TCP 계층에서 직접 판단하는 오류:

- JSON line이 너무 길면 `BAD_REQUEST`.
- JSON 파싱 실패는 `BAD_REQUEST`.
- `id` 누락은 `BAD_REQUEST`.
- 같은 connection의 in-flight 요청과 `id`가 중복되면 `BAD_REQUEST`.
- `op` 누락 또는 알 수 없는 `op`는 `BAD_REQUEST`.
- `op=sql`에서 `sql` 누락은 `BAD_REQUEST`.
- `sql` 길이가 `processor->context->max_sql_len`을 넘으면 `SQL_TOO_LONG`.
- `max_inflight_per_client`를 넘으면 `BUSY` 또는 `BAD_REQUEST`.

`CmdProcessor.process()` 이후:

- processor가 반환한 status를 JSON 응답에 그대로 반영한다.
- processor 호출 중 TCP 연결이 끊기면 TCP 계층은 응답 전송만 포기한다.
- 이미 시작된 processor 처리를 취소할지는 processor 구현체 책임이다.

## 12. Thread Safety 경계

TCP 구현체가 보호할 것:

- client connection 목록.
- connection 종료 상태.
- connection별 in-flight 요청 ID 목록.
- 같은 `client_fd`에 대한 response write.

TCP 구현체가 보호하지 않는 것:

- DB 파일 상태.
- SQL 실행 순서.
- 여러 요청 간 transaction 격리.
- `processor->context->shared_state` 내부 thread safety.

## 13. 테스트 계획

테스트에는 `cmd_processor/mock_cmd_processor`를 사용한다. 실제 DB parser/executor는 호출하지 않는다.

필수 테스트:

- server를 `port=0`으로 시작하고 `tcp_server_get_port()`로 접속 포트를 얻는다.
- 정상 `ping` 요청이 응답된다.
- 정상 `sql` 요청이 mock processor까지 전달된다.
- 한 connection에서 이전 응답을 기다리지 않고 여러 요청을 연속 전송할 수 있다.
- 응답의 `id`가 원래 요청과 일치한다.
- 잘못된 JSON은 `BAD_REQUEST`.
- `id` 누락은 `BAD_REQUEST`.
- 같은 connection의 in-flight `id` 중복은 `BAD_REQUEST`.
- `op` 누락은 `BAD_REQUEST`.
- 알 수 없는 `op`는 `BAD_REQUEST`.
- `op=sql`에서 `sql` 누락은 `BAD_REQUEST`.
- `CmdProcessorContext.max_sql_len`보다 긴 SQL은 `SQL_TOO_LONG`.
- `max_inflight_per_client` 초과는 `BUSY` 또는 `BAD_REQUEST`.
- `close` 요청은 해당 connection만 닫고 server는 계속 새 connection을 받을 수 있다.

검증 명령:

```sh
make test-cmd-processor
make test-tcp-frontend
make build
```

## 14. 최종 결정

TCP 관련 최종 계약은 아래와 같다.

```text
TCP 구현체는 tcp_frontend/ 폴더에 둔다.
TCP 구현체는 CmdProcessor public wrapper API만 호출한다.
TCP 구현체는 SQL parser/executor를 직접 호출하지 않는다.
TCP 구현체는 DB 동시성 정책을 강제하지 않는다.
TCP 요청은 JSON Lines로 받는다.
TCP 응답도 JSON Lines로 반환한다.
클라이언트는 요청마다 고유 id를 만들고 응답 id로 매핑한다.
서버는 connection과 in-flight id, socket write 공유만 관리한다.
DB 내부 병렬 실행과 lock 정책은 DB 구현체 책임이다.
```
