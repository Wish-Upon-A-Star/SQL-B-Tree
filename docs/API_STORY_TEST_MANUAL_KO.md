# API Story Test 시연 메뉴얼

## 목적

`test.sh`는 수요 코딩회 과제의 핵심 요구사항인 API 서버, 외부 클라이언트, 스레드풀 기반 요청 처리, 내부 SQL 엔진/B+Tree 연동을 한 번에 보여주는 발표용 시연 스크립트다.

시연 상황은 “과일모찌 팝업스토어 키오스크 주문 API”다. 키오스크 주문을 SQL 요청으로 보고, TCP API 서버가 주문 접수/조회/수정/취소/중복 픽업번호 방어/동시 주문 처리를 검증한다.

테스트에 등장하는 실제 메뉴는 다음 5개다.

- 망고 모찌
- 블루베리 모찌
- 딸기 모찌
- 망고 포멜로 모찌
- 누텔라 두바이 모찌

기본 시연인 `./test.sh`만 실행해도 5개 메뉴가 모두 성공 주문 데이터에 남는다. `./test.sh --stress`는 기능 시연을 반복하지 않고, 키오스크 4대가 각자 여러 주문을 동시에 outstanding 상태로 보내며 같은 메뉴 5개를 50,000건 동안 순환 INSERT하는 병렬 부하 시연이다.

## 파일과 저장 위치

추적되는 파일:

- `test.sh`: 발표자가 실행하는 진입점
- `tests/api_story_test.c`: TCP API 서버와 외부 클라이언트 역할을 함께 수행하는 C 시연 러너
- `docs/API_STORY_TEST_MANUAL_KO.md`: 이 메뉴얼

실행 중 생성되는 파일:

- `artifacts/api_story_test/api_story_test`: 빌드된 시연 러너
- `artifacts/api_story_test/output.log`: 발표용 실행 로그
- `artifacts/api_story_test/runtime/fruit_mochi_orders.csv`: 테스트용 DB 테이블
- `artifacts/api_story_test/runtime/fruit_mochi_orders.delta`: UPDATE/DELETE delta log
- `artifacts/api_story_test/runtime/fruit_mochi_orders.idx`: 인덱스 스냅샷

`artifacts/`는 `.gitignore`에 들어 있으므로 실행 결과물이 Git 변경사항으로 올라가지 않는다.

## 실행 방법

기능 시연:

```bash
./test.sh
```

병렬 부하 시연:

```bash
./test.sh --stress
```

`--stress` 기본값은 50,000건이다. 이 명령은 기능 시연 장면을 다시 실행하지 않고 부하 장면만 실행한다.

개발용 더 큰 부하 주문 수 직접 지정:

```bash
MOCHI_STRESS_ORDERS=60000 ./test.sh --stress
# 또는
./test.sh --orders=60000
```

60,000건 이상은 환경에 따라 10초를 넘길 수 있으므로 개발 검증용으로 사용한다.

생성 파일 정리:

```bash
./test.sh --clean
```

## 기능 시연 진행 순서

1. 명령어를 보여준다.

```bash
./test.sh
```

2. 첫 장면을 설명한다.

```text
[SCENE 1] 과일모찌 팝업스토어 키오스크 API 서버 오픈
[INFO] 메뉴판: 망고 모찌, 블루베리 모찌, 딸기 모찌, 망고 포멜로 모찌, 누텔라 두바이 모찌
```

멘트:

> 테스트 스크립트가 C 러너를 빌드하고, 러너가 내부에서 TCP API 서버를 실제로 띄웁니다. 이후 외부 클라이언트처럼 socket으로 JSON 요청을 보내 ping 응답을 확인합니다.

3. CRUD 장면을 설명한다.

```text
[SCENE 2] 첫 손님이 딸기 모찌를 주문
[PASS] INSERT -> SELECT -> UPDATE -> DELETE 전체 흐름이 API 요청으로 통과했습니다.
```

멘트:

> 주문 접수는 INSERT, 주문 조회는 SELECT, 제조 상태 변경은 UPDATE, 픽업 완료는 DELETE로 검증했습니다. 전부 CLI가 아니라 TCP API 요청으로 실행됩니다.

4. 제약조건 장면을 설명한다.

```text
[SCENE 3] 품절 직전, 같은 픽업번호를 두 번 받은 손님 등장
[INFO] 망고 포멜로 모찌 주문을 먼저 접수하고, 같은 픽업번호의 누텔라 두바이 모찌 주문을 일부러 다시 보냅니다.
[INFO] 아래 [error]는 중복 픽업번호 차단을 검증하기 위한 의도된 DB 엔진 로그입니다.
[INFO] 누텔라 두바이 모찌는 새 픽업번호로 재주문해 정상 접수되는지 확인합니다.
[PASS] B+ Tree UK 인덱스가 중복 픽업번호를 막고, 새 픽업번호 주문은 통과시켰습니다.
```

멘트:

> `pickup_code(UK)` 컬럼으로 같은 픽업번호가 두 번 발급되는 사고를 막습니다. 같은 픽업번호를 다른 주문 ID로 다시 넣으면 DB 엔진이 UK 중복을 감지하고 API는 실패 응답을 돌려줍니다. 이후 누텔라 두바이 모찌를 새 픽업번호로 다시 넣어서 정상 주문은 계속 처리됨을 확인합니다. 이때 출력되는 `[error]`는 테스트 실패가 아니라 의도된 차단 로그입니다.

5. 잘못된 요청 장면을 설명한다.

```text
[SCENE 4] 터치패널이 삐끗해서 이상한 JSON과 빈 주문서를 전송
[PASS] 잘못된 API 요청은 DB 엔진까지 내려가기 전에 거절됩니다.
```

멘트:

> malformed JSON, SQL이 없는 요청처럼 API 계층에서 막아야 하는 입력을 검증합니다. 키오스크 장애나 클라이언트 버그가 나도 DB 엔진에 넘기기 전에 BAD_REQUEST로 정리됩니다.

6. 동시성 장면을 설명한다.

```text
[SCENE 5] 팝업 마감 10초 전, 키오스크 여러 대에서 주문 버튼 연타
[INFO] 동시 주문 메뉴: 망고 모찌, 블루베리 모찌, 딸기 모찌, 망고 포멜로 모찌
[INFO] total_requests=...
[PASS] 여러 외부 클라이언트 요청이 request id별로 정상 처리됐습니다.
```

멘트:

> 여러 클라이언트 thread가 동시에 TCP 연결을 열고 망고 모찌, 블루베리 모찌, 딸기 모찌, 망고 포멜로 모찌 주문을 보냅니다. API 서버는 요청 id별 응답을 돌려주고, 내부 EngineCmdProcessor의 worker/queue 구조로 요청을 처리합니다.

7. 마지막 결과를 보여준다.

```text
[RESULT] PASS - 과일모찌 키오스크 API 서버 기능 시연 테스트 성공
```

멘트:

> 이 테스트는 API 서버, 외부 클라이언트, 내부 DB 엔진, B+Tree 인덱스, 동시 요청 처리를 한 흐름으로 검증합니다.

## 병렬 부하 시연 진행 순서

1. 기능 시연과 별도 명령임을 보여준다.

```bash
./test.sh --stress
```

2. 병렬 부하 장면을 설명한다.

```text
[SCENE 1] 과일모찌 팝업스토어 키오스크에 주문 50000건이 몰리는 부하 시연
[INFO] 부하 주문은 키오스크 4대가 window=16로 5개 메뉴를 순환 INSERT합니다.
[INFO] stress orders completed=1000
[INFO] stress orders completed=...
[INFO] stress orders completed=50000
[INFO] stress stats total_requests=... peak_request_slots=... max_queue_depth=...
[INFO] stress elapsed=... sec throughput=... orders/sec
[PASS] 대량 주문 후에도 PK 기반 API 조회가 정상 응답했습니다.
[RESULT] PASS - 과일모찌 키오스크 API 서버 병렬 부하 테스트 성공
```

멘트:

> 발표용 부하 시연은 기본 50,000건입니다. 한 클라이언트가 하나씩 보내고 기다리는 구조가 아니라, 키오스크 4대가 각자 16개까지 요청을 outstanding 상태로 밀어 넣어 API 서버의 in-flight 처리와 EngineCmdProcessor의 스레드풀/큐를 함께 검증합니다. `peak_request_slots`와 `max_queue_depth`가 1보다 크게 나오면 병렬 요청이 실제로 쌓였다는 근거로 설명할 수 있습니다. 마지막 PK 조회까지 성공하므로 대량 INSERT 이후에도 인덱스 기반 조회가 정상 동작함을 보여줍니다.

## QnA 대비 포인트

- API 형식: TCP newline-delimited JSON 요청이다. 예: `{"id":"order-1","op":"sql","sql":"INSERT ..."}`
- 응답 형식: JSON으로 `id`, `ok`, `status`, `body`, `row_count`, `affected_count`를 돌려준다.
- DB 저장: 테스트용 CSV는 `artifacts/api_story_test/runtime/fruit_mochi_orders.csv`에 저장된다.
- 인덱스 검증: `id(PK)`, `pickup_code(UK)`, `phone(UK)` 컬럼을 사용한다.
- 동시성 검증: 여러 pthread 클라이언트가 동시에 TCP 요청을 보내고, 서버 응답을 request id별로 검증한다.
- 부하 검증: `--stress`는 키오스크 4대, connection당 window 16개로 최대 64개 in-flight 요청을 만들어 스레드풀과 큐 사용을 `peak_request_slots`, `max_queue_depth`로 관찰한다.
- 시연 러너 설정: EngineCmdProcessor worker는 2개, request slot은 96개로 잡아 병렬 요청이 큐까지 들어가는 모습을 확인할 수 있게 했다.
- `./test.sh`와 `./test.sh --stress`는 의도적으로 장면을 겹치지 않게 분리했다.
- 발표에서 기능 시연은 `./test.sh`, 성능 시연은 `./test.sh --stress`를 사용한다.
- `MOCHI_STRESS_ORDERS=60000 ./test.sh --stress`는 더 큰 개발 검증용이다.
