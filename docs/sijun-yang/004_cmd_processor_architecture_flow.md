# CmdProcessor 전체 아키텍처와 요청 처리 흐름

이 문서는 TCP 서버만 설명하지 않는다. `CmdProcessor` 인터페이스를 중심으로 외부 진입점, 구현체, DB 실행 계층이 어떻게 분리되는지 정리한다.

다이어그램 이미지는 `docs/sijun-yang/diagrams/*.dot`을 원본으로 생성한다.

## 1. 문서 범위

`CmdProcessor`는 TCP, CLI, 테스트 코드 같은 외부 진입점과 실제 SQL 실행 계층 사이에 놓인 공통 요청 처리 계약이다.

```text
외부 진입점
        -> CmdProcessor wrapper API
        -> CmdProcessor 구현체
        -> DB 실행 계층
        -> CmdResponse callback
        -> 외부 응답 형식
```

현재 코드에서 주요 구성은 다음과 같다.

| 구성 | 위치 | 역할 |
| --- | --- | --- |
| `CmdProcessor` | `cmd_processor/cmd_processor.h` | 공통 request/response 계약과 vtable |
| `TCPCmdProcessor` | `cmd_processor/tcp_cmd_processor.c` | TCP JSONL을 `CmdRequest`/callback으로 변환하는 네트워크 어댑터 |
| `EngineCmdProcessor` | `cmd_processor/engine_cmd_processor*.c` | worker queue, planner cache, lock manager, SQL 실행을 가진 운영용 구현체 |
| `REPLCmdProcessor` | `cmd_processor/repl_cmd_processor.c` | REPL 방식의 동기 실행 구현체 |
| `MockCmdProcessor` | `cmd_processor/mock_cmd_processor.c` | API 계약 검증용 테스트 구현체 |
| DB 실행 계층 | `parser.c`, `executor.c`, `bptree.c` 등 | SQL 파싱, 실행, 테이블/B+Tree 데이터 처리 |

## 2. CmdProcessor 전체 아키텍처

![CmdProcessor 전체 아키텍처](./diagrams/004_cmd_processor_overall_architecture.svg)

DOT 원본: [`004_cmd_processor_overall_architecture.dot`](./diagrams/004_cmd_processor_overall_architecture.dot)

`CmdProcessor` 인터페이스는 실제 구현체를 숨기는 vtable이다. 외부 진입점은 `cmd_processor_acquire_request()`, `cmd_processor_set_sql_request()`, `cmd_processor_submit()` 같은 wrapper만 호출한다. 구현체 내부 상태는 `processor->context->shared_state` 뒤에 숨겨져 있고, TCP나 CLI는 이 값을 해석하지 않는다.

### CmdProcessor 인터페이스의 역할

- request/response 객체의 소유권 경계를 정한다.
- SQL 길이, request id 복사, ping/sql request 설정 같은 공통 helper를 제공한다.
- `submit()`과 response callback으로 요청 제출과 응답 수신을 분리한다.
- `CmdStatusCode`, `CmdBodyFormat`으로 외부 응답 변환에 필요한 최소 공통 표현을 제공한다.

`cmd_processor_submit()`의 반환값은 요청 처리 결과가 아니라 “제출 자체의 성공 여부”다. SQL parse 실패, DB busy, 실행 오류 같은 결과는 callback으로 전달되는 `CmdResponse.status`에 담긴다.

### 구현체의 역할

구현체는 `CmdProcessor` vtable 뒤에서 실제 정책을 결정한다.

- request/response slot을 얼마나 둘지 결정한다.
- 요청을 즉시 실행할지, worker queue에 넣을지 결정한다.
- SQL 요청의 라우팅, lock, 실행 순서를 결정한다.
- 처리 완료 시 callback을 호출하고, 원래 request id를 response에 보존한다.
- `release_request()`와 `release_response()`에서 processor-owned buffer를 회수한다.

`EngineCmdProcessor`는 운영용 구현체다. request/response slot pool, worker queue, planner cache, lock manager, metrics를 가진다. SQL 요청은 계획을 만든 뒤 필요하면 queue에 들어가고, worker가 lock을 잡은 뒤 `parser`/`executor` 계층을 호출한다. 현재 구현은 실제 `parse_statement()`와 `executor_execute_statement()` 호출 구간을 `engine_mutex`로 보호한다.

`REPLCmdProcessor`는 REPL에 맞춘 가벼운 구현체다. 요청을 받은 자리에서 동기적으로 처리하고 바로 응답을 돌려준다. 그래도 request/response 소유권 규칙은 동일하다.

### DB 실행 계층의 역할

DB 실행 계층은 SQL 의미와 데이터 정합성을 책임진다.

- SQL 문자열을 `Statement`로 파싱한다.
- `INSERT`, `SELECT`, `UPDATE`, `DELETE`를 실제 테이블 데이터에 적용한다.
- B+Tree, CSV/delta/index 파일, 테이블 생명주기 같은 저장 구조를 관리한다.
- 실행 결과를 `matched_rows`, `affected_rows`, `generated_id` 같은 구조화된 값으로 돌려준다.

TCP 계층은 DB lock, SQL 실행 순서, 테이블 데이터 구조를 직접 다루지 않는다.

## 3. TCP 기반 전체 요청 흐름

![TCP 기반 전체 요청 흐름](./diagrams/004_tcp_cmd_processor_architecture_flow.svg)

DOT 원본: [`004_tcp_cmd_processor_architecture_flow.dot`](./diagrams/004_tcp_cmd_processor_architecture_flow.dot)

이 그림은 여러 TCP connection에서 들어온 요청이 `TCPCmdProcessor`의 connection layer와 req/res adapter를 지나 DB 처리 쪽으로 위임되는 전체 흐름을 보여준다. 여기서 DB는 `CmdProcessor` 구현체 뒤쪽의 실행 계층을 단순화한 표현이다.

중요한 점은 `TCPCmdProcessor`가 DB 자체가 아니라는 것이다. TCP 계층은 JSON line을 읽고, 요청을 검증하고, `CmdRequest`를 만들어 `CmdProcessor`에 제출한다. 응답은 callback으로 돌아오며, TCP 계층은 그 응답을 다시 JSON line으로 직렬화해서 같은 connection에 쓴다.

## 4. 공통 요청 처리 계약

외부 진입점의 기본 흐름은 다음과 같다.

```text
cmd_processor_acquire_request()
        -> cmd_processor_set_sql_request() 또는 cmd_processor_set_ping_request()
        -> cmd_processor_submit(callback, user_data)
        -> callback에서 외부 응답 형식으로 변환
        -> cmd_processor_release_response()
        -> cmd_processor_release_request()
```

소유권 규칙은 단순하다.

| 객체 | 소유자 | 외부 진입점이 할 일 |
| --- | --- | --- |
| `CmdRequest` | `CmdProcessor` 구현체 | acquire 후 채우고, callback에서 release |
| `CmdResponse` | `CmdProcessor` 구현체 | callback에서 읽고 release |
| `user_data` | 외부 진입점 | callback이 응답을 되돌릴 대상을 찾는 데 사용 |

`submit()` 성공 후에는 request를 즉시 해제하지 않는다. request는 callback이 호출될 때까지 구현체 내부에서 유효해야 한다. callback은 response를 TCP JSONL, CLI 출력, 테스트 검증용 구조 등 외부 형식으로 바꾼 뒤 request/response를 반환한다.

## 5. 사용자가 TCP connection을 요청할 때의 흐름

![TCP connection accept 흐름](./diagrams/004_tcp_connection_accept_flow.svg)

DOT 원본: [`004_tcp_connection_accept_flow.dot`](./diagrams/004_tcp_connection_accept_flow.dot)

connection 요청이 들어오면 accept thread가 다음 순서로 처리한다.

1. `accept(listen_fd)`로 새 `client_fd`를 얻는다.
2. client 주소에서 `client_key`를 만든다. IPv4/IPv6 주소 문자열이 key가 된다.
3. `reserve_connection_slot()`에서 서버 전체 connection 수와 client별 connection 수를 검사한다.
4. 제한을 넘으면 fd를 닫고 종료한다.
5. 통과하면 socket read/write timeout을 설정한다.
6. `TCPConnection`을 만들고 `server->connections` linked list에 추가한다.
7. connection 전용 thread를 detach 상태로 시작한다.
8. connection thread는 newline 단위 JSON request를 반복해서 읽는다.

현재 기본 제한은 다음과 같다.

| 제한 | 기본값 | 의미 |
| --- | ---: | --- |
| `TCP_MAX_CONNECTIONS_TOTAL` | 128 | 서버 전체 active connection 수 |
| `TCP_MAX_CONNECTIONS_PER_CLIENT` | 4 | 같은 client key의 connection 수 |
| `TCP_READ_TIMEOUT_MS` | 30000 | read timeout |
| `TCP_WRITE_TIMEOUT_MS` | 30000 | write timeout |

## 6. TCPCmdProcessor가 저장하는 상태와 request 반환 흐름

![TCPCmdProcessor 상태와 request lifecycle](./diagrams/004_tcp_cmd_processor_state_lifecycle.svg)

DOT 원본: [`004_tcp_cmd_processor_state_lifecycle.dot`](./diagrams/004_tcp_cmd_processor_state_lifecycle.dot)

`TCPCmdProcessor`가 직접 저장하는 데이터는 TCP 서버와 connection 관리용 메타데이터다.

| 위치 | 저장 데이터 | 사용 목적 |
| --- | --- | --- |
| `TCPCmdProcessor` | `listen_fd`, `actual_port`, `stopping`, `active_clients` | 서버 socket과 종료 상태 관리 |
| `TCPCmdProcessor` | `connections` | active `TCPConnection` linked list |
| `TCPCmdProcessor` | `client_counters` | client별 connection/in-flight 수 제한 |
| `TCPCmdProcessor` | `processor` | 요청을 위임할 `CmdProcessor *` |
| `TCPConnection` | `client_fd`, `client_key`, `closing` | 특정 TCP 연결 상태 |
| `TCPConnection` | `inflight_count`, `inflight_ids` | 같은 connection 안에서 완료 전인 request id 추적 |
| `TCPConnection` | `ref_count` | callback 완료 전 connection 해제를 막는 참조 수 |
| `TCPConnection` | `state_mutex`, `write_mutex` | 상태 변경과 응답 쓰기 직렬화 |
| `TCPClientCounter` | `connection_count`, `inflight_count` | client 단위 제한 적용 |

요청이 들어오면 TCP 계층은 먼저 `register_inflight()`를 호출한다. 이 함수는 다음 조건을 확인한 뒤 request id를 connection의 `inflight_ids`에 추가한다.

- connection이 닫히는 중이면 `BUSY`
- 같은 connection에 같은 in-flight id가 이미 있으면 `BAD_REQUEST`
- connection별 in-flight 수가 `TCP_MAX_INFLIGHT_PER_CONNECTION` 이상이면 `BUSY`
- client별 in-flight 수가 `TCP_MAX_INFLIGHT_PER_CLIENT` 이상이면 `BUSY`

등록이 성공하면 `TCPConnection.inflight_count`와 `TCPClientCounter.inflight_count`가 함께 증가한다. 이후 `CmdRequest`를 확보하고, JSON 요청을 `CMD_REQUEST_SQL` 또는 `CMD_REQUEST_PING`으로 복사한 뒤 `cmd_processor_submit()`에 callback과 connection pointer를 넘긴다.

응답이 돌아오면 `tcp_response_callback()`이 실행된다.

1. `CmdResponse`를 JSON object로 직렬화한다.
2. connection의 `write_mutex`를 잡고 JSON line을 쓴다.
3. `remove_inflight()`로 request id를 제거하고 connection/client counter를 감소시킨다.
4. `cmd_processor_release_response()`와 `cmd_processor_release_request()`를 호출한다.
5. callback 동안 붙잡아 둔 connection ref를 반환한다.

## 7. 여러 요청이 동시에 들어온 상황

![TCP 여러 in-flight 요청 흐름](./diagrams/004_tcp_multi_request_inflight_flow.svg)

DOT 원본: [`004_tcp_multi_request_inflight_flow.dot`](./diagrams/004_tcp_multi_request_inflight_flow.dot)

한 connection은 이전 요청의 응답을 기다리는 동안 다음 JSON line을 계속 읽을 수 있다. 그래서 같은 connection 안에 여러 in-flight request id가 동시에 존재할 수 있다.

응답 순서는 요청 순서와 항상 같지 않다. `CmdProcessor` 구현체가 `r2`를 `r1`보다 먼저 끝내면 TCP 계층은 `r2` 응답을 먼저 쓴다. 클라이언트는 JSON response의 `id`로 어떤 요청의 결과인지 매칭해야 한다.

동시에 같은 connection에서 같은 `id`를 재사용하면 `duplicate in-flight request id` 오류가 난다. 이미 완료되어 `inflight_ids`에서 제거된 id는 다시 사용할 수 있지만, 클라이언트 관점에서는 request id를 계속 유니크하게 만드는 편이 디버깅에 유리하다.

## 8. TCP 요청/응답 JSON 형식

TCP protocol은 newline으로 구분되는 JSON object, 즉 JSONL 방식이다. 각 request와 response는 한 줄이다.

### 요청 형식

| 필드 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `id` | string | 예 | 요청 식별자. 1바이트 이상, 최대 63바이트 |
| `op` | string | 예 | `"sql"`, `"ping"`, `"close"` 중 하나 |
| `sql` | string | `op="sql"`일 때 | 실행할 SQL 문자열. processor의 `max_sql_len` 이하 |

요청 예:

```json
{"id":"s1","op":"sql","sql":"SELECT * FROM users;"}
{"id":"p1","op":"ping"}
{"id":"c1","op":"close"}
```

`close`는 현재 connection만 닫는다. 서버나 다른 connection을 종료하지 않는다.

### 응답 형식

| 필드 | 타입 | 조건 | 설명 |
| --- | --- | --- | --- |
| `id` | string | 항상 | 요청 id. id를 읽지 못한 오류는 `"unknown"` |
| `ok` | boolean | 항상 | `status == "OK"`이면 `true` |
| `status` | string | 항상 | `CmdStatusCode` 문자열 |
| `row_count` | number | 0이 아닐 때 | 조회/매칭 row 수 |
| `affected_count` | number | 0이 아닐 때 | 변경된 row 수 |
| `body` | string/object/array | body가 있을 때 | `CMD_BODY_TEXT`는 string, `CMD_BODY_JSON`은 JSON 값 |
| `error` | string | 실패 응답일 때 | 오류 메시지 |

현재 status 문자열은 다음 값을 쓴다.

```text
OK
BAD_REQUEST
SQL_TOO_LONG
PARSE_ERROR
PROCESSING_ERROR
BUSY
TIMEOUT
INTERNAL_ERROR
UNKNOWN
```

`TIMEOUT`은 enum에는 예약되어 있지만 v1 TCP 흐름에서 직접 발생시키지는 않는다.

응답 예:

```json
{"id":"p1","ok":true,"status":"OK","body":"pong"}
{"id":"s1","ok":true,"status":"OK","row_count":3,"body":"SELECT matched_rows=3"}
{"id":"s2","ok":true,"status":"OK","body":{"sql":"SELECT 1;"}}
{"id":"s3","ok":false,"status":"BAD_REQUEST","error":"missing sql"}
{"id":"r3","ok":false,"status":"BUSY","error":"too many in-flight requests on connection"}
```

## 9. 번외: REPLCmdProcessor 내부 구조

![REPLCmdProcessor 내부 구조](./diagrams/004_repl_cmd_processor_structure.svg)

DOT 원본: [`004_repl_cmd_processor_structure.dot`](./diagrams/004_repl_cmd_processor_structure.dot)

`REPLCmdProcessor`는 TCP 서버가 아니라 `CmdProcessor` 구현체다. REPL frontend가 콘솔 입력을 SQL 요청으로 만들면, REPL 구현체가 동기적으로 처리하고 결과를 다시 콘솔 출력으로 돌려준다.

구조는 단순하다.

- `REPLCmdProcessorState`가 processor-owned state를 가진다.
- adapter 영역은 콘솔에서 넘어온 입력을 `CmdRequest`로 만들고 결과를 `CmdResponse`로 돌려준다.
- execution 영역은 ping과 SQL 요청을 동기적으로 처리한다.
- DB 영역은 이 그림에서 BlackBox로 둔다.
- 응답이 전달된 뒤 외부 진입점이 request/response를 release한다.

REPL은 네트워크 framing, in-flight id list, client별 제한을 갖지 않는다. 대신 processor mutex로 실행 구간을 직렬화하고, 요청 처리와 응답 전달이 한 흐름 안에서 끝나는 동기 실행 모델을 사용한다.
