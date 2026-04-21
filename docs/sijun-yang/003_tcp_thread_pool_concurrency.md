# TCP Connection 동시성 핵심 개념

## 1. 목적

이 문서는 API/TCP 계층에서 알아야 할 connection 동시성 개념을 간단히 정리한다.

DB 내부 구현, SQL 실행 방식, B+ Tree lock 정책은 DB 구현 담당자의 책임이다. API/TCP 계층은 여러 클라이언트 요청을 받아 `CmdProcessor.process()`까지 전달하는 역할에 집중한다.

```text
TCP client
        -> TCP server
        -> CmdProcessor.process()
        -> DB 구현체
```

## 2. 클라이언트와 서버

TCP 통신은 두 프로그램 사이의 연결이다.

- 클라이언트는 요청을 보내는 프로그램이다.
- 서버는 요청을 받아 처리하고 응답을 보내는 프로그램이다.
- 이 프로젝트에서 클라이언트는 DBMS 기능을 사용하려는 외부 프로그램이다.
- 이 프로젝트에서 서버는 TCP JSONL API 서버다.

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

같은 TCP connection이라도 클라이언트와 서버는 각자 자기 프로세스 안의 socket fd를 가진다. 클라이언트 fd와 서버 fd는 같은 숫자일 필요가 없고, 서로 공유되는 값도 아니다.

## 3. Acceptor와 처리 계층 분리

TCP 서버는 연결을 받는 역할, 요청을 읽는 역할, DB 처리 계층에 요청을 넘기는 역할을 분리한다.

```text
acceptor
        -> connection reader
        -> CmdProcessor
```

### 3.1 Socket FD의 종류와 수명

- `listen_fd`는 새 TCP 연결을 받기 위한 서버 socket이다.
- `client_fd`는 특정 클라이언트와 실제로 통신하는 socket이다.
- TCP 계층은 `client_fd`에서 여러 요청 line을 읽어 `CmdProcessor`에 전달한다.

FD 개수와 공유:

| FD | 개수 | 공유 여부 |
|---|---|---|
| `listen_fd` | 서버 인스턴스당 보통 1개 | acceptor가 주로 사용한다. |
| server-side `client_fd` | client connection마다 1개 | reader와 응답 writer들이 같은 fd를 공유한다. |
| client-side socket fd | 클라이언트 프로세스의 connection마다 1개 | 클라이언트 내부에서만 사용하는 fd다. |

주의할 점:

- 클라이언트의 socket fd와 서버의 `client_fd`는 같은 TCP 연결의 양 끝이지만 같은 FD 값은 아니다.
- `client_fd` 하나에 여러 in-flight 요청이 걸릴 수 있다.
- 같은 `client_fd`에 여러 응답이 비동기적으로 쓰일 수 있으므로 write는 서버가 보호해야 한다.

### 3.2 연결 단위 처리와 요청 단위 처리

- v1 설계는 한 connection에서 여러 요청이 동시에 처리 중일 수 있다.
- 한 connection에서 이전 응답을 기다리지 않고 다음 요청을 보낼 수 있다.
- 응답 순서는 보장하지 않고, 요청 `id`로 매핑한다.

### 3.3 In-flight 요청 수와 동시 처리량

- TCP 계층은 connection별 in-flight 요청 수를 제한해야 한다.
- 실제 SQL 처리량은 DB 구현체의 병렬 처리 정책과 lock 정책에 좌우된다.

## 4. 공유 상태와 동기화

TCP 계층도 공유 상태를 가진다. 다만 SQL 실행 병렬화는 DB 구현체 책임이다.

```text
connection reader
        -> in-flight 요청 등록
        -> CmdProcessor에 전달
        -> response write
```

### 4.1 Critical Section

- client 수, in-flight 요청 목록, connection 종료 상태는 여러 thread가 함께 보는 공유 상태다.
- 이런 공유 상태를 읽거나 바꾸는 짧은 구간을 critical section이라고 한다.
- critical section은 mutex로 보호한다.

### 4.2 Socket Write 보호

- 같은 `client_fd`에 여러 응답이 쓰일 수 있다.
- 응답 JSON line이 섞이지 않도록 write 구간은 서버가 보호해야 한다.
- 이 보호는 TCP connection 상태 관리의 일부다.

### 4.3 Backpressure와 종료 신호

- client 수와 connection별 in-flight 요청 수에는 제한이 있어야 한다.
- 제한을 넘는 요청은 거절해 서버 자원을 보호한다.
- 서버 종료 시에는 열린 connection과 처리 중인 요청 상태를 정리해야 한다.

## 5. TCP 계층 동시성과 DB 계층 동시성 경계

TCP 계층은 여러 connection에서 여러 요청을 `CmdProcessor`까지 전달할 수 있게 한다.

```text
connection A -> request 1 -> CmdProcessor.process()
connection A -> request 2 -> CmdProcessor.process()
connection B -> request 3 -> CmdProcessor.process()
```

실제 SQL 실행을 병렬로 할지, lock으로 직렬화할지는 DB 구현체가 결정한다.

같은 connection의 응답 순서는 보장하지 않는다. 클라이언트는 요청마다 고유 `id`를 만들고 응답의 `id`로 원래 요청을 찾는다.

### 5.1 CmdProcessor 동시 호출 계약

- 여러 connection과 여러 in-flight 요청 때문에 `CmdProcessor.process()`는 동시에 호출될 수 있다.
- DB 구현체는 이 동시 호출을 안전하게 처리해야 한다.
- 병렬 처리가 어렵다면 내부에서 직렬화하거나 `BUSY`, `TIMEOUT` 같은 상태를 반환할 수 있다.

### 5.2 요청/응답 소유권 경계

- TCP 계층은 요청을 파싱하고 `CmdProcessor`에 전달한다.
- `CmdRequest`, `CmdResponse`의 실제 소유권은 `CmdProcessor`에 둔다.
- 이렇게 해야 여러 요청이 동시에 처리되어도 요청/응답 메모리 관리 책임이 명확하다.
- TCP 계층은 in-flight 요청 ID를 관리해 응답 매핑을 가능하게 한다.

### 5.3 오류 책임과 상태 전파

- TCP 계층은 잘못된 JSON, 필드 누락, 너무 긴 요청 같은 입력 오류를 처리한다.
- SQL 문법 오류, 실행 오류, DB busy, timeout은 `CmdProcessor` 또는 DB 구현체가 판단한다.
- TCP 계층은 `CmdProcessor`가 반환한 상태를 JSON 응답으로 변환한다.

## 6. 클라이언트와 서버 책임

클라이언트 책임:

- 요청마다 고유한 `id`를 만든다.
- 이전 응답을 기다리지 않고 여러 요청을 보낼 수 있다.
- `id -> 요청 상태` 매핑을 관리한다.
- 응답 순서가 달라도 `id`로 결과를 찾는다.

서버 책임:

- connection에서 요청 line을 계속 읽는다.
- 요청을 `CmdProcessor`에 전달한다.
- 같은 connection의 in-flight `id` 중복을 막는다.
- 응답에 원래 요청 `id`를 넣는다.
- 같은 `client_fd`에 대한 동시 write를 안전하게 보호한다.

## 7. 핵심 요약

```text
TCP 서버는 acceptor, connection reader, CmdProcessor 호출 경계를 분리한다.
클라이언트는 id를 만들고 응답을 매핑한다.
서버는 id 중복과 socket write 공유를 관리한다.
한 connection에서 여러 요청이 동시에 in-flight 상태가 될 수 있다.
응답 순서는 보장하지 않으며 id로 요청과 응답을 매핑한다.
SQL 병렬 실행과 lock 정책은 DB 구현체 책임이다.
```
