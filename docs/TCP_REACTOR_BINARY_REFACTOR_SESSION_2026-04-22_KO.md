# TCP Reactor + Binary Refactor Session Notes

## 목적

이번 회차의 목적은 기존 `thread-per-connection + JSONL` TCP 경로를
`Linux epoll reactor + binary framing + worker는 DB만` 구조로 바꾸기 위한
실제 작업 경계를 문서로 고정하고, 그 문서를 기준으로 구현을 시작하는 것이다.

이 문서는 설계 문서가 아니라 이번 구현 회차의 작업 지시서다.
즉, 각 파일의 어느 부분을 어떻게 바꿀지와 이번 회차에서 실제로 끝낼 1차 작업을 적는다.

## 최종 목표

최종 목표는 아래와 같다.

1. TCP front-end는 connection thread를 없애고 reactor thread만 사용한다.
2. 외부 TCP 프로토콜은 JSONL을 버리고 binary frame으로 완전 교체한다.
3. DB worker는 socket read/write를 하지 않고 SQL 실행과 결과 handoff만 담당한다.
4. reactor가 connection 상태, frame decode, outbound flush를 담당한다.
5. hot connection 또는 slow client가 DB worker를 막지 않게 한다.

## 이번 회차에서 실제로 할 일

이번 회차는 전체 구조를 한 번에 다 바꾸지 않는다.
누더기처럼 중간 상태가 길게 남지 않도록, 아래 순서로 경계를 먼저 만든다.

### 1단계. protocol 분리

새 파일을 추가한다.

- `cmd_processor/tcp_protocol_binary.h`
- `cmd_processor/tcp_protocol_binary.c`

이 파일들이 담당할 것:

- request/response frame header 정의
- little-endian encode/decode helper
- frame 길이 검증
- request id / payload body offset 계산
- 상태 코드와 body format을 binary response header에 싣는 helper

이번 단계에서는 protocol 자체를 `tcp_cmd_processor.c` 바깥으로 빼는 것이 핵심이다.
나중에 reactor가 붙더라도 binary frame 규약을 다시 정의하지 않게 만든다.

### 2단계. public TCP interface 정리

수정 파일:

- `cmd_processor/tcp_cmd_processor.h`

바꿀 내용:

- JSON line 전용 상수 이름 제거 준비
- binary frame 기반으로 필요한 상수 이름을 추가
- TCP config에 protocol-aware 제약을 둘 수 있게 구조를 정리

이번 단계에서는 기존 API 함수를 유지한다.

- `tcp_cmd_processor_config_init`
- `tcp_cmd_processor_start`
- `tcp_cmd_processor_get_port`
- `tcp_cmd_processor_stop`

즉 public entrypoint는 유지하되, 내부 구현이 JSON 전용이라는 가정을 header에서 줄인다.

### 3단계. 빌드 경계 추가

수정 파일:

- `Makefile`

바꿀 내용:

- `tcp_protocol_binary.c`를 TCP server/test 빌드에 포함
- reactor 분리 파일을 추가할 준비가 되도록 TCP deps를 더 명시적으로 분리

이번 회차에서는 실제 reactor 파일까지 다 만들지 않더라도,
`Makefile`이 새 TCP 하위 모듈을 자연스럽게 포함하도록 정리한다.

### 4단계. protocol 단위 테스트 추가

수정 파일:

- `cmd_processor/tcp_cmd_processor_test.c`

이번 회차 테스트 목표:

- binary request header encode/decode round-trip
- binary response header encode/decode round-trip
- invalid magic/version/length rejection
- request id/body offset 계산 검증

이번 단계에서는 기존 JSON TCP 동작 테스트를 당장 제거하지 않는다.
대신 binary protocol 단위 테스트를 먼저 추가해서 protocol contract를 고정한다.

### 5단계. 다음 회차를 위한 실제 삽입 지점 표시

수정 파일:

- `cmd_processor/tcp_cmd_processor.c`

이번 회차에서 이 파일에 할 일:

- 현재 JSON parse/build 전용 책임이 어디 있는지 기준 함수를 유지
- 다음 교체 대상 경계를 코드상으로 분명히 한다

다음 회차 교체 대상:

- `read_json_line`
- `parse_request_line`
- `process_json_line`
- `build_status_json`
- `build_cmd_response_json`
- `tcp_response_callback`
- `connection_thread_main`
- `accept_thread_main`

이번 회차에서는 이 함수들을 한 번에 없애지 않는다.
대신 새 protocol 모듈을 붙일 준비가 되도록 include와 helper 경계를 정리한다.

## 파일별 수정 예정 위치

### `cmd_processor/tcp_protocol_binary.h`

새로 추가한다.

포함 내용:

- request/response magic 상수
- protocol version 상수
- op enum
- packed frame header struct
- encode/decode 함수 선언

### `cmd_processor/tcp_protocol_binary.c`

새로 추가한다.

포함 내용:

- little-endian read/write helper
- header size 검사
- request frame decode
- response frame build helper

### `cmd_processor/tcp_cmd_processor.h`

수정 위치:

- 기존 TCP 상수 정의 블록
- `TCPCmdProcessorConfig`

변경 방향:

- line/json 전용 냄새를 줄이고 binary protocol이 전제되는 상수 추가

### `cmd_processor/tcp_cmd_processor.c`

수정 위치:

- 상단 include 구간
- local helper 선언 구간

변경 방향:

- 새 protocol 모듈 include
- 향후 protocol decode/build 경계를 별도 모듈로 넘기기 쉬운 형태로 준비

이번 회차에서는 동작 전체를 binary로 갈아엎지 않는다.
그 대신 기존 JSON 구현을 유지한 채 protocol 모듈을 먼저 독립시킨다.

### `cmd_processor/tcp_cmd_processor_test.c`

수정 위치:

- 상단 include
- helper 영역
- main test run 영역

변경 방향:

- cJSON 기반 응답 파싱 테스트는 그대로 두되
- binary protocol encode/decode 단위 테스트를 추가

### `Makefile`

수정 위치:

- `TCP_SERVER_DEPS`
- `TCP_CMD_PROCESSOR_TEST_SRC`

변경 방향:

- 새 protocol 파일이 TCP server/test 빌드에 들어가도록 추가

## 이번 회차에서 하지 않는 것

이번 회차에서는 아래를 일부러 하지 않는다.

- epoll reactor 본체 전체 구현
- connection outbound queue 전체 구현
- eventfd handoff 구현
- thread-per-connection 완전 삭제
- 기존 JSON TCP 동작 제거

이유:

- protocol contract와 빌드 경계를 먼저 고정하지 않으면
  reactor 본체를 넣는 동안 diff가 커지고 다시 누더기처럼 될 가능성이 높다.

## 완료 판정

이번 회차 완료 기준은 아래다.

1. binary protocol 모듈이 별도 파일로 생긴다.
2. TCP 빌드와 테스트가 새 protocol 모듈을 포함해 통과한다.
3. binary protocol 단위 테스트가 추가된다.
4. 다음 회차 reactor 치환 지점이 문서와 코드에서 모두 분명하다.

## 다음 회차 시작점

다음 회차는 이 문서를 그대로 이어서 아래부터 시작한다.

1. `tcp_reactor_linux.c` 추가
2. `tcp_cmd_processor.c`에서 connection thread 경로 제거 시작
3. binary frame read path 연결
4. worker callback -> completion queue handoff 분리

## Session Progress Update

- done
  - `cmd_processor/tcp_protocol_binary.h` added
  - `cmd_processor/tcp_protocol_binary.c` added
  - `Makefile` updated so TCP server/test targets include the protocol module
  - `cmd_processor/tcp_cmd_processor_test.c` now speaks the binary wire protocol for ping/sql/close/limit checks
  - `cmd_processor/tcp_cmd_processor.h` gained `TCP_MAX_FRAME_BYTES`
  - `cmd_processor/tcp_cmd_processor.c` no longer reads JSON lines; it now reads fixed binary headers and binary payload frames
  - `cmd_processor/tcp_cmd_processor.c` no longer builds JSON responses; it now serializes binary response frames directly
  - `cmd_processor/tcp_cmd_processor.c` no longer lets worker callbacks write sockets directly; responses are queued on the connection and flushed by the connection thread
  - `cmd_processor/tcp_cmd_processor.c` now has a connection-owned wakeup path, so response flushing is separated from request execution
- next
  - introduce Linux reactor file and acceptor/reactor split
  - remove thread-per-connection in favor of reactor-owned connection state
  - replace per-connection wakeup + poll loop with reactor-owned event loop
- current verification note
  - full TCP build is still blocked in the current Windows environment because the TCP source depends on POSIX headers such as `arpa/inet.h`
  - the new protocol module itself was smoke-compiled and executed successfully
