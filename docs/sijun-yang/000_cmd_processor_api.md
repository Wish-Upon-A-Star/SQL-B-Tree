# CmdProcessor 비동기 요청 처리 API 설계 계획

## 1. 목적

`CmdProcessor`는 TCP, CLI, HTTP 같은 외부 진입점과 DB 구현체 사이의 공통 요청 처리 인터페이스다.

이번 계약의 핵심은 **요청 제출과 응답 수신을 분리**하는 것이다.

```text
외부 진입점
        -> CmdProcessor.submit()
        -> DB 구현체
        -> response callback
        -> 외부 진입점
```

이 구조를 두면 TCP 계층은 connection에서 다음 요청을 계속 읽을 수 있고, DB 실행 병렬화와 lock 정책은 DB 구현체 내부에 둘 수 있다.

## 2. 책임 경계

외부 진입점 책임:

- `CmdRequest`를 확보한다.
- 요청 ID와 SQL 문자열을 request에 복사한다.
- `cmd_processor_submit()`으로 요청을 제출한다.
- callback에서 `CmdResponse`를 외부 응답 형식으로 변환한다.
- callback에서 `CmdRequest`와 `CmdResponse`를 release한다.

`CmdProcessor` 구현체 책임:

- 제출된 요청을 처리한다.
- 처리 완료 시 callback을 호출한다.
- 응답에 원래 요청 ID를 보존한다.
- 내부 실행을 병렬화할지, 직렬화할지, `BUSY`/`TIMEOUT`으로 거절할지 결정한다.

외부 진입점이 하지 않는 일:

- DB lock 관리.
- DB 내부 queue 관리.
- SQL 실행 순서 결정.
- `processor->context->shared_state` 해석.

## 3. API 계약

공용 헤더는 `cmd_processor/cmd_processor.h`다.

핵심 타입:

```c
typedef void (*CmdProcessorResponseCallback)(
    CmdProcessor *processor,
    CmdRequest *request,
    CmdResponse *response,
    void *user_data
);
```

핵심 vtable:

```c
typedef struct CmdProcessor {
    CmdProcessorContext *context;

    int (*acquire_request)(CmdProcessorContext *context,
                           CmdRequest **out_request);

    int (*submit)(CmdProcessor *processor,
                  CmdProcessorContext *context,
                  CmdRequest *request,
                  CmdProcessorResponseCallback callback,
                  void *user_data);

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

외부 진입점이 호출하는 wrapper:

```c
int cmd_processor_acquire_request(CmdProcessor *processor,
                                  CmdRequest **out_request);

CmdStatusCode cmd_processor_set_sql_request(CmdProcessor *processor,
                                            CmdRequest *request,
                                            const char *request_id,
                                            const char *sql);

CmdStatusCode cmd_processor_set_ping_request(CmdProcessor *processor,
                                             CmdRequest *request,
                                             const char *request_id);

int cmd_processor_submit(CmdProcessor *processor,
                         CmdRequest *request,
                         CmdProcessorResponseCallback callback,
                         void *user_data);

int cmd_processor_make_error_response(CmdProcessor *processor,
                                      const char *request_id,
                                      CmdStatusCode status,
                                      const char *error_message,
                                      CmdResponse **out_response);

void cmd_processor_release_request(CmdProcessor *processor,
                                   CmdRequest *request);

void cmd_processor_release_response(CmdProcessor *processor,
                                    CmdResponse *response);
```

## 4. 요청 제출 흐름

```text
외부 진입점
        -> cmd_processor_acquire_request()
        -> cmd_processor_set_sql_request() 또는 cmd_processor_set_ping_request()
        -> cmd_processor_submit(callback, user_data)
        -> 즉시 다음 외부 요청 처리 가능

CmdProcessor 구현체
        -> 내부 정책에 따라 요청 처리
        -> 완료 시 callback(processor, request, response, user_data)
```

`cmd_processor_submit()`의 반환값은 “요청 제출 자체”의 성공 여부다.

- `0`: 요청이 구현체에 제출됐다.
- `-1`: API 사용 오류 또는 제출 실패다. 이 경우 callback은 호출되지 않는다.

SQL 문법 오류, DB busy, timeout 같은 정상적인 요청 실패는 callback으로 전달되는 `CmdResponse.status`에 담는다.

## 5. Callback 소유권 규칙

callback이 받는 객체:

```text
request  -> CmdProcessor 소유
response -> CmdProcessor 소유
```

callback 책임:

- response를 TCP JSONL, HTTP response, CLI 출력 등 외부 형식으로 변환한다.
- `cmd_processor_release_response()`를 호출한다.
- `cmd_processor_release_request()`를 호출한다.

규칙:

- 외부 진입점은 `submit()` 성공 후 request를 즉시 release하지 않는다.
- request는 callback이 호출될 때까지 유효해야 한다.
- response가 `NULL`이면 치명적 내부 실패로 보고 외부 진입점이 최소 오류 응답을 만든다.
- callback은 긴 작업을 피하고 필요한 작업을 빠르게 끝내는 것이 좋다.

## 6. TCP 사용 예시

```text
connection read loop
        -> JSON line read
        -> request id 검증
        -> CmdRequest 생성
        -> cmd_processor_submit(tcp_response_callback, connection)
        -> 다음 JSON line read

tcp_response_callback
        -> response id 확인
        -> connection write lock
        -> JSON line response write
        -> in-flight id 제거
        -> request/response release
```

이 흐름에서는 TCP 계층이 DB 실행 worker를 만들지 않는다. 실제 실행 병렬화는 `CmdProcessor` 구현체 내부에서 결정한다.

## 7. Mock 구현체

`mock_cmd_processor`는 callback 계약을 검증하기 위한 구현체다.

- `submit()`을 호출하면 mock은 즉시 응답을 만들고 callback을 호출한다.
- 실제 DB queue나 비동기 스레드를 만들지는 않는다.
- callback 기반 API의 소유권, request ID 보존, response release 흐름을 테스트한다.

## 8. 테스트 기준

테스트할 항목:

- `ping` 요청 submit 후 callback이 호출된다.
- `sql` 요청 submit 후 callback 응답에 같은 request ID가 보존된다.
- callback에서 request/response를 release할 수 있다.
- SQL 문자열은 request setter가 processor-owned buffer로 복사한다.
- SQL 길이 제한을 setter에서 검증한다.
- foreign request를 submit하면 실패하고 callback은 호출되지 않는다.
- 여러 호출자가 동시에 submit해도 response/request 메모리가 오염되지 않는다.
- `BUSY`, `TIMEOUT`, `PROCESSING_ERROR`가 callback response status로 전달된다.

## 9. 최종 결정

```text
CmdProcessor는 동기 process API가 아니라 submit + callback API를 제공한다.
submit 성공은 요청 제출 성공만 의미한다.
요청 처리 결과는 callback의 CmdResponse로 전달한다.
request/response 소유권은 CmdProcessor에 있다.
callback은 response 변환 후 request/response를 release한다.
DB 실행 병렬화와 lock 정책은 CmdProcessor 구현체 책임이다.
```
