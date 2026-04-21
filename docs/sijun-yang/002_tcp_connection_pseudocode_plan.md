# TCP Connection 구현 의사코드 계획

## 1. 목적

이 문서는 TCP JSONL API 서버의 구현 흐름을 의사코드로 정리한다.

범위는 TCP/API 계층이다.

```text
TCP client
        -> TCP server
        -> CmdProcessor
        -> DB 구현체
```

TCP 계층은 사용자별 TCP connection을 맺고, JSON line 요청을 읽어 `CmdProcessor`로 전달한다. SQL 실행 병렬화, DB lock, 실제 처리 순서는 DB 구현체 책임이다.

## 2. 핵심 전제

- 클라이언트는 TCP connection을 열고 JSON line 요청을 보낸다.
- 요청마다 클라이언트가 고유 `id`를 만든다.
- 서버는 응답에 같은 `id`를 넣는다.
- 응답 순서는 보장하지 않는다.
- 서버는 connection별 in-flight `id` 중복을 막는다.
- 서버는 같은 `client_fd`에 대한 응답 write가 섞이지 않게 보호한다.

중요한 제약:

- TCP 계층은 DB 실행 구조를 만들지 않는다.
- 다중 in-flight를 진짜로 지원하려면 `CmdProcessor`가 요청 제출 후 나중에 응답을 돌려줄 수 있어야 한다.
- `CmdProcessor.process()`가 blocking 함수라면 같은 connection의 요청 처리는 순차 처리로 축소된다.

## 3. 주요 상태

```python
서버 = {
    "listen_fd": 서버_소켓,
    "중지중": False,
    "활성_클라이언트_수": 0,
    "연결목록": [],
    "processor": CmdProcessor,
    "설정": {
        "max_clients": 128,
        "max_line_bytes": ...,
        "max_inflight_per_client": 16,
    },
}
```

```python
연결 = {
    "client_fd": 클라이언트_소켓,
    "닫는중": False,
    "처리중_id들": set(),
    "상태_lock": Lock(),
    "쓰기_lock": Lock(),
}
```

## 4. 서버 시작

```python
def 서버_시작(설정):
    설정을_검증한다()
    processor가_있는지_확인한다()

    서버를_생성한다()
    listen_fd를_연다()
    주소와_포트를_bind한다()
    listen을_시작한다()

    accept_loop를_시작한다()

    return 서버
```

세부 socket option이나 메모리 할당 방식은 구현 단계에서 정한다.

## 5. Accept Loop

```python
def accept_loop(서버):
    while not 서버["중지중"]:
        client_fd = 새_connection을_accept한다()

        if accept에_실패했다():
            if 서버["중지중"]:
                break
            continue

        if 서버가_받을_수_있는_클라이언트_수를_넘었다():
            client_fd를_닫는다()
            continue

        연결 = 연결을_생성한다(client_fd)
        서버_연결목록에_추가한다(연결)

        connection_read_loop를_시작한다(연결)
```

accept loop는 새 TCP connection을 받는 역할만 한다.

## 6. Connection Read Loop

```python
def connection_read_loop(연결):
    while not 연결["닫는중"]:
        line = JSON_line을_읽는다(연결["client_fd"])

        if 연결이_끊겼다(line):
            break

        요청 = 요청을_파싱한다(line)

        if 요청이_잘못됐다(요청):
            오류를_응답한다(연결, 요청.id, "BAD_REQUEST")
            continue

        if 요청.op == "close":
            성공을_응답한다(연결, 요청.id)
            break

        if not 처리중_id를_등록한다(연결, 요청.id):
            오류를_응답한다(연결, 요청.id, "BAD_REQUEST")
            continue

        processor로_요청을_전달한다(연결, 요청)

    연결을_정리한다(연결)
```

이 loop는 connection에서 계속 요청을 읽는다. 요청 처리 완료를 기다릴지, 바로 다음 요청을 읽을지는 `CmdProcessor` 경계가 blocking인지 async인지에 따라 달라진다.

## 7. In-flight ID 관리

```python
def 처리중_id를_등록한다(연결, 요청_id):
    with 연결["상태_lock"]:
        if 연결["닫는중"]:
            return False

        if 요청_id in 연결["처리중_id들"]:
            return False

        if len(연결["처리중_id들"]) >= max_inflight_per_client:
            return False

        연결["처리중_id들"].add(요청_id)
        return True
```

```python
def 처리중_id를_제거한다(연결, 요청_id):
    with 연결["상태_lock"]:
        연결["처리중_id들"].remove(요청_id)
```

서버가 in-flight ID를 관리하는 이유는 응답 순서를 보장하지 않기 때문이다.

## 8. CmdProcessor 전달 경계

```python
def processor로_요청을_전달한다(연결, 요청):
    processor = 서버의_processor를_가져온다(연결)

    cmd_request = CmdRequest를_확보한다(processor)

    if cmd_request를_확보하지_못했다():
        오류를_응답한다(연결, 요청.id, "INTERNAL_ERROR")
        처리중_id를_제거한다(연결, 요청.id)
        return

    요청_내용을_CmdRequest에_복사한다(cmd_request, 요청)

    if 요청_복사에_실패했다():
        응답 = processor_오류_응답을_만든다(processor, 요청.id)
        processor_응답을_처리한다(연결, cmd_request, 응답)
        return

    CmdProcessor에_요청을_제출한다(processor, cmd_request)
```

동기 `CmdProcessor.process()`만 사용할 수 있다면 마지막 줄은 아래 의미가 된다.

```python
응답 = CmdProcessor가_요청을_처리할_때까지_기다린다(cmd_request)
processor_응답을_처리한다(연결, cmd_request, 응답)
```

async 경계가 있다면 아래 의미가 된다.

```python
CmdProcessor에_요청만_넘기고_즉시_return한다()
나중에_processor_응답을_처리한다(연결, cmd_request, 응답)
```

TCP 계층은 여기서 SQL 실행 순서, DB lock, 병렬 처리 방식을 정하지 않는다.

## 9. 응답 처리

```python
def processor_응답을_처리한다(연결, cmd_request, 응답):
    if 응답이_없다():
        오류를_응답한다(연결, cmd_request.id, "INTERNAL_ERROR")
    else:
        정상_응답을_쓴다(연결, 응답)

    CmdResponse를_해제한다()
    CmdRequest를_해제한다()
    처리중_id를_제거한다(연결, cmd_request.id)
```

응답 처리의 핵심은 원래 요청 ID를 유지하는 것이다.

## 10. Socket Write 보호

```python
def 정상_응답을_쓴다(연결, 응답):
    json_line = 응답을_JSON_line으로_바꾼다(응답)

    with 연결["쓰기_lock"]:
        if not 연결["닫는중"]:
            client_fd에_쓴다(연결["client_fd"], json_line)
```

```python
def 오류를_응답한다(연결, 요청_id, 상태):
    json_line = 오류를_JSON_line으로_바꾼다(요청_id, 상태)

    with 연결["쓰기_lock"]:
        client_fd에_쓴다(연결["client_fd"], json_line)
```

같은 `client_fd`에 여러 응답이 쓰일 수 있으므로 write 구간은 보호해야 한다.

## 11. 요청 파싱과 검증

```python
def 요청을_파싱한다(line):
    JSON_object로_파싱한다()
    id를_읽는다()
    op를_읽는다()
    sql을_읽는다()
    return 요청
```

```python
def 요청이_잘못됐다(요청):
    if id가_없다():
        return True

    if op가_지원되지_않는다():
        return True

    if op가_sql인데_sql이_없다():
        return True

    if sql이_너무_길다():
        return True

    return False
```

세부 JSON escape 처리나 parser 구현 방식은 구현 단계에서 정한다.

## 12. Connection 종료

```python
def 연결을_정리한다(연결):
    연결["닫는중"] = True

    client_fd를_shutdown한다()
    client_fd를_close한다()

    서버_연결목록에서_제거한다(연결)
    연결_자원을_해제한다()
```

종료 규칙:

- `op=close`는 현재 connection만 닫는다.
- server 전체 shutdown이 아니다.
- 이미 `CmdProcessor`로 넘어간 요청을 취소할지는 DB 구현체 책임이다.

## 13. 서버 종료

```python
def 서버를_중지한다(서버):
    서버["중지중"] = True

    listen_fd를_닫는다()

    for 연결 in 서버["연결목록"]:
        연결["닫는중"] = True
        연결의_client_fd를_닫는다()

    서버_자원을_해제한다()
```

## 14. 테스트 계획

```python
def 포트_0으로_서버를_시작할_수_있다():
    서버 = 서버_시작(port=0)
    assert 실제_포트(서버) > 0
```

```python
def ping_요청에_응답한다():
    클라이언트가_연결한다()
    요청을_보낸다({"id": "p1", "op": "ping"})
    응답 = 응답을_읽는다()
    assert 응답["id"] == "p1"
```

```python
def sql_요청을_processor로_전달한다():
    클라이언트가_연결한다()
    요청을_보낸다({"id": "s1", "op": "sql", "sql": "SELECT * FROM users;"})
    응답 = 응답을_읽는다()
    assert 응답["id"] == "s1"
```

```python
def 같은_connection에서_여러_요청을_보낼_수_있다():
    클라이언트가_연결한다()
    요청을_보낸다({"id": "1", "op": "ping"})
    요청을_보낸다({"id": "2", "op": "ping"})
    응답들 = 응답_두_개를_읽는다()
    assert 응답_id_집합(응답들) == {"1", "2"}
```

```python
def 처리중_id가_중복되면_거절한다():
    클라이언트가_연결한다()
    요청을_보낸다({"id": "dup", "op": "sql", "sql": "SELECT 1;"})
    요청을_보낸다({"id": "dup", "op": "sql", "sql": "SELECT 2;"})
    assert 두번째_요청은_BAD_REQUEST()
```

```python
def close는_현재_connection만_닫는다():
    클라이언트_A가_연결한다()
    클라이언트_B가_연결한다()

    클라이언트_A가_close를_보낸다()

    assert 클라이언트_A는_닫힌다()
    assert 클라이언트_B는_계속_요청할_수_있다()
```

## 15. 최종 결정

```text
TCP 계층은 사용자별 connection을 관리한다.
TCP 계층은 JSON line 요청을 읽고 CmdProcessor로 전달한다.
TCP 계층은 request id와 in-flight 상태를 관리한다.
TCP 계층은 같은 client_fd에 대한 response write를 보호한다.
TCP 계층은 DB 내부 실행 구조를 알지 않는다.
SQL 실행 병렬화와 lock 정책은 DB 구현체 책임이다.
```
