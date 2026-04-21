# TCP Connection 구현 실행 계획

## 1. 목적

이 문서는 TCP JSONL API 서버를 구현할 때 따라야 할 실행 계획과 구현 조건을 정리한다.

`002_tcp_connection_pseudocode_plan.md`가 전체 흐름을 설명한다면, 이 문서는 실제 구현 전에 확정해야 할 값, 책임, 상태, 오류 조건, 테스트 기준을 더 구체화한다.

코드는 작성하지 않는다.

## 2. 구현 범위

TCP 계층이 구현할 것:

- TCP listen socket 생성.
- client connection accept.
- connection별 read loop.
- JSON line 단위 요청 수신.
- 요청 ID 검증과 in-flight ID 관리.
- `CmdProcessor.submit()` 호출.
- response callback에서 JSON line 응답 작성.
- 같은 `client_fd`에 대한 write 보호.
- connection 종료와 서버 종료 정리.

TCP 계층이 구현하지 않을 것:

- SQL parser 호출.
- B+ Tree 접근.
- DB 파일 접근.
- SQL 실행 순서 결정.
- DB lock 관리.
- DB 내부 병렬 처리 구조.

## 3. 매크로로 조정할 값

아래 값은 소스 상단의 매크로로 조정 가능해야 한다. 런타임 설정 구조체가 생기더라도, 기본값은 매크로에서 온다.

```c
#define TCP_DEFAULT_HOST "127.0.0.1"
#define TCP_DEFAULT_PORT 0
#define TCP_DEFAULT_BACKLOG 16
#define TCP_MAX_CONNECTIONS_TOTAL 128
#define TCP_MAX_CONNECTIONS_PER_CLIENT 4
#define TCP_MAX_INFLIGHT_PER_CONNECTION 16
#define TCP_MAX_INFLIGHT_PER_CLIENT 64
#define TCP_MAX_LINE_BYTES 8192
#define TCP_READ_TIMEOUT_MS 30000
#define TCP_WRITE_TIMEOUT_MS 30000
#define TCP_REQUEST_ID_MAX_BYTES 63
#define TCP_OP_MAX_BYTES 15
#define TCP_ERROR_MESSAGE_MAX_BYTES 256
```

각 값의 의미:

| 매크로 | 의미 |
|---|---|
| `TCP_DEFAULT_HOST` | 기본 bind 주소. |
| `TCP_DEFAULT_PORT` | 기본 port. `0`이면 OS가 빈 port를 배정한다. |
| `TCP_DEFAULT_BACKLOG` | listen backlog 기본값. |
| `TCP_MAX_CONNECTIONS_TOTAL` | 서버가 동시에 유지할 수 있는 client socket 총 개수. |
| `TCP_MAX_CONNECTIONS_PER_CLIENT` | 같은 client가 동시에 열 수 있는 socket 최대 수. |
| `TCP_MAX_INFLIGHT_PER_CONNECTION` | connection 하나에서 동시에 처리 중일 수 있는 요청 최대 수. |
| `TCP_MAX_INFLIGHT_PER_CLIENT` | 같은 client의 모든 connection을 합친 동시 요청 최대 수. |
| `TCP_MAX_LINE_BYTES` | JSON line 하나의 최대 byte 수. |
| `TCP_READ_TIMEOUT_MS` | 요청 line read timeout. |
| `TCP_WRITE_TIMEOUT_MS` | response write timeout. |
| `TCP_REQUEST_ID_MAX_BYTES` | request `id` 최대 byte 수. |
| `TCP_OP_MAX_BYTES` | `op` 문자열 최대 byte 수. |
| `TCP_ERROR_MESSAGE_MAX_BYTES` | TCP 계층 기본 오류 메시지 최대 byte 수. |

주의:

- `TCP_MAX_LINE_BYTES`는 `processor->context->max_sql_len`보다 작으면 안 된다.
- 실제 SQL 길이 제한은 최종적으로 `processor->context->max_sql_len`을 따른다.
- `TCP_MAX_CONNECTIONS_PER_CLIENT`는 `TCP_MAX_CONNECTIONS_TOTAL`보다 클 수 없다.
- `TCP_MAX_INFLIGHT_PER_CONNECTION`은 `TCP_MAX_INFLIGHT_PER_CLIENT`보다 클 수 없다.
- v1에서 client 구분 기준은 remote address다.

## 4. 데이터 구조 명세

### 4.1 TcpServer

서버는 아래 상태를 가진다.

| 상태 | 의미 |
|---|---|
| `listen_fd` | 새 connection을 받는 socket fd. |
| `actual_port` | 실제 bind된 port. |
| `stopping` | 서버 종료 중인지 여부. |
| `active_clients` | 현재 열린 client connection 수. |
| `connections` | 열린 connection 목록. |
| `client_counters` | client별 열린 socket 수와 in-flight 수. |
| `clients_mutex` | `active_clients`, `connections`, `client_counters` 보호용 lock. |
| `processor` | 외부에서 주입받은 `CmdProcessor`. |

조건:

- `processor`는 필수다.
- `processor->context`도 필수다.
- `TcpServer`는 `CmdProcessor`를 소유하지 않는다.
- `tcp_server_stop()`은 `cmd_processor_shutdown()`을 호출하지 않는다.
- `client_counters`는 `clients_mutex`로 보호한다.

### 4.2 TcpConnection

connection은 아래 상태를 가진다.

| 상태 | 의미 |
|---|---|
| `client_fd` | 특정 클라이언트와 통신하는 socket fd. |
| `client_key` | client를 구분하기 위한 값. v1에서는 remote address. |
| `closing` | connection 종료 중인지 여부. |
| `inflight_ids` | 아직 응답이 완료되지 않은 요청 ID 집합. |
| `ref_count` | callback 지연 도착에 대비한 connection 객체 수명 카운터. |
| `state_mutex` | `closing`, `inflight_ids`, `ref_count` 보호용 lock. |
| `write_mutex` | 같은 `client_fd`에 대한 response write 보호용 lock. |

조건:

- connection 하나는 server-side `client_fd` 하나를 가진다.
- 같은 client는 여러 `client_fd`를 가질 수 있다.
- `client_fd` 하나에는 여러 in-flight 요청이 존재할 수 있다.
- 같은 `client_fd`에 여러 response가 쓰일 수 있으므로 write는 반드시 보호한다.
- submit 성공 후 callback이 남아 있으면 connection 객체를 해제하지 않는다.

### 4.3 Socket FD 조건

FD 종류:

| FD | 의미 |
|---|---|
| `listen_fd` | 서버당 하나. 새 connection을 받기 위한 socket. |
| `client_fd` | accept 결과로 생기는 server-side socket. connection마다 하나. |

조건:

- 열린 상태의 `client_fd`는 하나의 `TcpConnection`에만 속한다.
- 같은 client는 여러 `client_fd`를 열 수 있다.
- 닫힌 FD 번호는 OS가 재사용할 수 있으므로 영구 ID로 쓰지 않는다.
- 요청 매핑은 FD가 아니라 request `id`와 connection 상태로 한다.

## 5. 구현 단계

### 5.1 1단계: 서버 생명주기

구현할 항목:

- `tcp_server_start()`
- `tcp_server_get_port()`
- `tcp_server_stop()`

완료 조건:

- port `0`으로 시작하면 실제 port를 조회할 수 있다.
- `processor == NULL`이면 start가 실패한다.
- `processor->context == NULL`이면 start가 실패한다.
- stop 이후 listen socket이 닫힌다.

### 5.2 2단계: Accept Loop

구현할 항목:

- `listen_fd`에서 client connection을 accept한다.
- remote address로 `client_key`를 만든다.
- `TCP_MAX_CONNECTIONS_TOTAL`을 넘으면 새 connection을 닫는다.
- `TCP_MAX_CONNECTIONS_PER_CLIENT`를 넘으면 새 connection을 닫는다.
- accepted `client_fd`마다 `TcpConnection`을 만든다.
- connection을 server connection 목록에 등록한다.
- 등록 시 `active_clients`와 client별 socket 수를 증가시킨다.

완료 조건:

- 여러 클라이언트가 동시에 연결될 수 있다.
- 최대 연결 수를 넘는 connection은 거절된다.
- 같은 client가 열 수 있는 connection 수가 제한된다.
- connection 종료 시 `active_clients`가 감소한다.
- 거절된 connection은 요청을 읽지 않고 닫는다.

### 5.3 3단계: JSON Line Read

구현할 항목:

- `client_fd`에서 `\n`까지 읽어 JSON line 하나를 만든다.
- `\r\n`이면 마지막 `\r`은 제거한다.
- `TCP_MAX_LINE_BYTES`를 넘으면 오류 응답을 보낸다.
- EOF 또는 read timeout이면 connection을 종료한다.

완료 조건:

- 한 connection에서 여러 JSON line을 연속으로 읽을 수 있다.
- line 단위 framing이 깨지지 않는다.
- 너무 긴 line은 서버 메모리를 계속 사용하지 않는다.

### 5.4 4단계: 요청 파싱과 검증

필수 요청 필드:

- `id`
- `op`
- `sql`, 단 `op == "sql"`일 때만 필수

허용 `op`:

- `sql`
- `ping`
- `close`

검증 조건:

- `id`가 없으면 `BAD_REQUEST`.
- `id`가 빈 문자열이면 `BAD_REQUEST`.
- `id`가 `TCP_REQUEST_ID_MAX_BYTES`를 넘으면 `BAD_REQUEST`.
- `id`는 client가 생성하고 server는 검증과 추적만 한다.
- 같은 connection의 in-flight ID와 중복되면 `BAD_REQUEST`.
- `op`가 없으면 `BAD_REQUEST`.
- `op`가 허용 목록 밖이면 `BAD_REQUEST`.
- `op == "sql"`인데 `sql`이 없으면 `BAD_REQUEST`.
- `sql` 길이가 `processor->context->max_sql_len`을 넘으면 `SQL_TOO_LONG`.

완료 조건:

- 잘못된 요청은 DB 구현체로 전달하지 않는다.
- TCP 계층에서 판단 가능한 오류는 TCP 계층에서 응답한다.

### 5.5 5단계: In-flight ID 관리

등록 시점:

- 요청 검증이 끝난 뒤.
- `CmdProcessor.submit()` 호출 직전.

제거 시점:

- response callback 처리가 끝난 뒤.
- submit 실패로 callback이 오지 않을 것이 확정된 뒤.
- connection 종료 후 늦게 온 callback이 처리된 뒤.

거절 조건:

- connection이 closing 상태다.
- 같은 ID가 이미 in-flight 상태다.
- 해당 connection의 in-flight 수가 `TCP_MAX_INFLIGHT_PER_CONNECTION` 이상이다.
- 해당 client의 전체 in-flight 수가 `TCP_MAX_INFLIGHT_PER_CLIENT` 이상이다.

완료 조건:

- 같은 connection에서 처리 중인 ID는 중복되지 않는다.
- 같은 client가 만들 수 있는 동시 요청 수가 제한된다.
- 응답 순서와 관계없이 ID로 요청 상태를 제거할 수 있다.
- 등록 시 connection/client in-flight 수를 증가시킨다.
- 제거 시 connection/client in-flight 수를 감소시킨다.
- in-flight 요청이 남아 있으면 connection 객체는 해제하지 않는다.

### 5.6 6단계: CmdProcessor.submit 연동

호출 흐름:

```text
CmdRequest 확보
요청 내용을 CmdRequest에 복사
cmd_processor_submit(processor, request, response_callback, connection_context)
```

submit 성공 조건:

- 반환값이 `0`이다.
- 성공한 submit은 callback을 정확히 한 번 호출해야 한다.
- request 소유권은 callback이 호출될 때까지 `CmdProcessor`에 남는다.
- TCP 계층은 submit 성공 직후 request를 release하지 않는다.

submit 실패 처리:

- callback이 호출되지 않는 것으로 본다.
- in-flight ID를 제거한다.
- 가능하면 `INTERNAL_ERROR` 응답을 보낸다.
- request를 확보했다면 release한다.

완료 조건:

- TCP 계층은 `processor->context->shared_state`를 해석하지 않는다.
- TCP 계층은 DB 처리 순서를 가정하지 않는다.
- response callback으로 돌아온 응답만 socket에 쓴다.

### 5.7 7단계: Response Callback

callback이 해야 할 일:

- `CmdResponse`를 JSON line으로 직렬화한다.
- connection의 `write_mutex`를 잡는다.
- `client_fd`에 response line을 쓴다.
- in-flight ID를 제거한다.
- `CmdResponse`를 release한다.
- `CmdRequest`를 release한다.

주의:

- callback은 어느 스레드에서 호출될지 TCP 계층이 가정하지 않는다.
- callback은 connection이 이미 closing 상태일 수 있음을 고려한다.
- write 실패 시 connection을 closing 상태로 바꾼다.

완료 조건:

- response에는 원래 요청 `id`가 포함된다.
- 같은 `client_fd`에 대한 response line이 섞이지 않는다.
- response callback 이후 request/response slot이 반환된다.

### 5.8 8단계: Connection 종료

종료 조건:

- client EOF.
- read timeout.
- write 실패.
- `op == "close"`.
- server stop.

종료 처리:

- `closing = true`로 만든다.
- 더 이상 새 요청을 받지 않는다.
- `client_fd`를 shutdown/close한다.
- server connection 목록에서 제거한다.
- `active_clients`를 감소시킨다.
- client별 socket 수를 감소시킨다.
- in-flight callback이 모두 끝난 뒤 connection 객체를 해제한다.

주의:

- 이미 `CmdProcessor.submit()`으로 넘어간 요청은 TCP 계층이 취소하지 않는다.
- callback이 늦게 도착할 수 있으므로 connection 수명 처리를 조심해야 한다.
- v1에서는 단순하게 connection 종료 중 도착한 callback은 socket write를 생략한다.

## 6. 오류 응답 정책

TCP 계층 오류는 아래 status로 응답한다.

| 상황 | status |
|---|---|
| JSON 파싱 실패 | `BAD_REQUEST` |
| 필수 필드 누락 | `BAD_REQUEST` |
| 알 수 없는 `op` | `BAD_REQUEST` |
| in-flight ID 중복 | `BAD_REQUEST` |
| connection별 in-flight 수 초과 | `BUSY` |
| client별 in-flight 수 초과 | `BUSY` |
| SQL 길이 초과 | `SQL_TOO_LONG` |
| request slot 확보 실패 | `INTERNAL_ERROR` |
| submit 실패 | `INTERNAL_ERROR` |
| response 직렬화 실패 | `INTERNAL_ERROR` |

응답 규칙:

- 가능한 경우 원 요청 `id`를 응답에 넣는다.
- `id`를 파싱하지 못한 경우 빈 문자열 또는 `"unknown"`을 사용한다.
- 오류 응답도 JSON line 하나로 보낸다.

## 7. JSON 응답 형식

공통 필드:

```json
{"id":"...","ok":true,"status":"OK"}
```

실패 응답:

```json
{"id":"...","ok":false,"status":"BAD_REQUEST","error":"..."}
```

body 처리:

- `CMD_BODY_NONE`: `body` 생략.
- `CMD_BODY_TEXT`: JSON string으로 escape.
- `CMD_BODY_JSON`: JSON value로 삽입.

주의:

- `CMD_BODY_JSON`은 processor가 유효한 JSON value를 만든다고 가정한다.
- TCP 계층은 text/error 문자열을 반드시 JSON escape한다.

## 8. 테스트 체크리스트

서버 생명주기:

- port `0`으로 시작 가능.
- 실제 port 조회 가능.
- stop 후 새 connection 실패.

connection:

- 단일 client 연결 가능.
- 여러 client 연결 가능.
- `TCP_MAX_CONNECTIONS_TOTAL` 초과 시 거절.
- `TCP_MAX_CONNECTIONS_PER_CLIENT` 초과 시 거절.

요청/응답:

- `ping` 요청 성공.
- `sql` 요청이 `CmdProcessor.submit()`까지 전달됨.
- 응답 `id`가 요청 `id`와 같음.
- 한 connection에서 여러 요청을 연속 전송 가능.
- 응답 순서가 달라도 ID로 매핑 가능.

오류:

- 잘못된 JSON은 `BAD_REQUEST`.
- `id` 누락은 `BAD_REQUEST`.
- `op` 누락은 `BAD_REQUEST`.
- 알 수 없는 `op`는 `BAD_REQUEST`.
- `sql` 누락은 `BAD_REQUEST`.
- SQL 길이 초과는 `SQL_TOO_LONG`.
- in-flight ID 중복은 `BAD_REQUEST`.
- connection별 in-flight 수 초과는 `BUSY`.
- client별 in-flight 수 초과는 `BUSY`.

종료:

- `op=close`는 해당 connection만 닫음.
- 한 client 종료가 다른 client에 영향 없음.
- write 실패 시 connection 정리.

## 9. 최종 결정

```text
TCP 계층은 connection과 JSONL framing을 관리한다.
TCP 계층은 request id와 in-flight 상태를 관리한다.
TCP 계층은 CmdProcessor.submit()까지만 요청을 전달한다.
TCP 계층은 response callback에서 socket write를 수행한다.
TCP 계층의 제한값은 매크로로 조정 가능해야 한다.
DB 내부 실행 구조와 lock 정책은 DB 구현체 책임이다.
```
