# TCPCmdProcessor Architecture Flow

## 1. 전체 구조

```mermaid
flowchart LR
    Client[TCP Client]
    Server[TCPCmdProcessor]
    Accept[accept thread]
    Conn[connection read thread]
    Parser[cJSON request parser]
    Inflight[in-flight id manager]
    Cmd[CmdProcessor.submit]
    DB[DB 구현체]
    Callback[tcp_response_callback]
    Writer[client_fd writer]

    Client -- JSON line --> Server
    Server --> Accept
    Accept -- accept client_fd --> Conn
    Conn --> Parser
    Parser --> Inflight
    Inflight --> Cmd
    Cmd --> DB
    DB -- CmdResponse --> Callback
    Callback --> Writer
    Writer -- JSON line --> Client
```

TCPCmdProcessor는 TCP 연결, JSONL framing, request id, socket write만 관리한다. DB 실행 순서와 lock 정책은 `CmdProcessor` 뒤쪽 구현체 책임이다.

## 2. 주요 상태

```mermaid
classDiagram
    class TCPCmdProcessor {
        listen_fd
        actual_port
        stopping
        accept_thread
        active_clients
        connections
        client_counters
        processor
        mutex
        cond
    }

    class TCPConnection {
        client_fd
        client_key
        closing
        inflight_count
        ref_count
        inflight_ids
        state_mutex
        write_mutex
    }

    class TCPClientCounter {
        client_key
        connection_count
        inflight_count
    }

    class CmdProcessor {
        context
        acquire_request()
        submit()
        release_request()
        release_response()
    }

    TCPCmdProcessor "1" --> "*" TCPConnection
    TCPCmdProcessor "1" --> "*" TCPClientCounter
    TCPCmdProcessor "1" --> "1" CmdProcessor
    TCPConnection "1" --> "*" TCPInflightId
```

## 3. 서버 시작과 연결 수락

```mermaid
sequenceDiagram
    participant Caller
    participant Server as TCPCmdProcessor
    participant OS
    participant Accept as accept_thread
    participant Conn as connection_thread

    Caller->>Server: tcp_cmd_processor_start(config)
    Server->>Server: validate_config()
    Server->>OS: socket/bind/listen
    Server->>Accept: pthread_create()
    Caller->>Server: tcp_cmd_processor_get_port()

    loop until stop
        Accept->>OS: accept(listen_fd)
        OS-->>Accept: client_fd
        Accept->>Server: reserve_connection_slot(client_key)
        Accept->>Conn: pthread_create(client_fd)
    end
```

연결 제한은 두 단계로 검사한다.

- 전체 socket 수: `TCP_MAX_CONNECTIONS_TOTAL`
- 같은 client의 socket 수: `TCP_MAX_CONNECTIONS_PER_CLIENT`

## 4. 요청 처리 Flow

```mermaid
flowchart TD
    Read[read JSON line]
    TooLong{line too long?}
    Parse{valid JSON object?}
    Fields{valid id/op/sql?}
    Close{op == close?}
    Register{register in-flight id}
    Acquire{acquire CmdRequest}
    SetReq[copy request into CmdRequest]
    Submit{CmdProcessor.submit}
    Error[write error JSON line]
    CloseConn[close current connection]
    Next[read next JSON line]

    Read --> TooLong
    TooLong -- yes --> Error --> Next
    TooLong -- no --> Parse
    Parse -- no --> Error --> Next
    Parse -- yes --> Fields
    Fields -- no --> Error --> Next
    Fields -- yes --> Close
    Close -- yes --> CloseConn
    Close -- no --> Register
    Register -- fail --> Error --> Next
    Register -- ok --> Acquire
    Acquire -- fail --> Error --> Next
    Acquire -- ok --> SetReq --> Submit
    Submit -- fail --> Error --> Next
    Submit -- ok --> Next
```

`submit()` 성공 후 connection thread는 응답을 기다리지 않고 다음 JSON line을 읽는다. 그래서 같은 connection에서 여러 요청이 동시에 in-flight 상태가 될 수 있다.

## 5. Callback 응답 Flow

```mermaid
sequenceDiagram
    participant Cmd as CmdProcessor
    participant Cb as tcp_response_callback
    participant Conn as TCPConnection
    participant Client

    Cmd-->>Cb: callback(processor, request, response, connection)
    Cb->>Cb: CmdResponse -> JSON line
    Cb->>Conn: lock write_mutex
    Conn-->>Client: write(client_fd, json + "\n")
    Cb->>Conn: unlock write_mutex
    Cb->>Conn: remove in-flight id
    Cb->>Cmd: release_response()
    Cb->>Cmd: release_request()
    Cb->>Conn: connection_release()
```

응답 순서는 보장하지 않는다. 클라이언트는 응답의 `id`로 원 요청을 매핑해야 한다.

## 6. 수명과 Lock 관계

```mermaid
flowchart LR
    ReadRef[read thread ref_count 1]
    SubmitRef[submit success callback ref_count +1]
    CallbackDone[callback done ref_count -1]
    ReadDone[read thread done ref_count -1]
    Destroy{ref_count == 0?}
    Free[free TCPConnection]

    ReadRef --> SubmitRef
    SubmitRef --> CallbackDone
    ReadRef --> ReadDone
    CallbackDone --> Destroy
    ReadDone --> Destroy
    Destroy -- yes --> Free
    Destroy -- no --> Wait[keep connection object alive]
```

```mermaid
flowchart TD
    ServerMutex[server mutex]
    StateMutex[connection state_mutex]
    WriteMutex[connection write_mutex]

    ServerMutex --> ServerState[connections, active_clients, client_counters]
    StateMutex --> ConnState[closing, ref_count, inflight_ids]
    WriteMutex --> SocketWrite[client_fd JSON line write]
```

핵심 규칙:

- in-flight 등록/제거는 server 상태와 connection 상태를 함께 갱신한다.
- 같은 `client_fd`에 대한 write는 `write_mutex`로 보호한다.
- callback이 늦게 와도 `ref_count` 때문에 connection 객체가 먼저 해제되지 않는다.

## 7. 종료 Flow

```mermaid
sequenceDiagram
    participant Caller
    participant Server as TCPCmdProcessor
    participant Accept as accept_thread
    participant Conn as TCPConnection

    Caller->>Server: tcp_cmd_processor_stop()
    Server->>Server: stopping = 1
    Server->>Accept: close listen_fd
    Server->>Accept: pthread_join()
    Server->>Conn: close all client_fd
    Conn-->>Server: connection_release()
    Server->>Server: wait until connections == NULL
    Server->>Server: destroy mutex/cond and free
```

`op == "close"`는 서버 전체 종료가 아니라 현재 connection만 닫는다.
