# SQLprocessor 스트레스 테스트 가이드

## 목적

이 문서는 현재 코드 기준으로 어떤 테스트를 어디까지 돌려야 하는지, 그리고 각 테스트가 무엇을 검증하는지 정리한다.

특히 다음 세 가지를 구분한다.

- 엔진 내부 성능 테스트
- 엔진 동시성 및 회귀 테스트
- 외부 TCP 기준 mixed CRUD 부하 테스트

## 테스트 분류

### 1. 엔진 마이크로 벤치

목적:

- B+Tree 인덱스 경로와 선형 스캔 경로의 차이를 빠르게 확인
- 대형 테이블에서 어떤 컬럼이 병목인지 식별

명령:

```powershell
.\stress_runner.exe --benchmark 1000000
```

주요 확인 포인트:

- `id SELECT using B+ tree`
- `email(UK) SELECT using B+ tree`
- `phone(UK) SELECT using B+ tree`
- `name SELECT using linear scan`
- scan 대비 index 속도 비율

이 테스트는 외부 TCP를 통하지 않는다. 따라서 DB 엔진 자체의 lookup/scan 특성 확인용이다.

### 2. 엔진 SQL 경로 벤치

목적:

- parser -> executor -> storage 전체 SQL 실행 경로 측정
- 데이터셋 생성, 로드, SELECT/UPDATE/DELETE 경로의 내부 비용 확인

명령:

```powershell
.\stress_runner.exe --benchmark-jungle 1000000
```

주요 확인 포인트:

- 대형 jungle 데이터셋 생성 시간
- `id/email/phone/github` exact-match 처리 시간
- `name` 같은 비인덱스 조건의 선형 스캔 시간
- `UPDATE/DELETE` 이후 인덱스와 delta 상태가 정상인지

이 테스트도 외부 TCP는 포함하지 않는다. DB 엔진 단독 성능과 회귀를 보는 용도다.

### 3. 엔진 멀티스레드 회귀 테스트

목적:

- worker thread와 lock 경계가 실제로 안전한지 확인
- 같은 id, 같은 unique 값, 같은 테이블 read/write 경합을 재현

빌드:

```powershell
gcc -O2 -fdiagnostics-color=always -g -Icmd_processor cmd_processor\engine_cmd_processor_test.c cmd_processor\cmd_processor.c cmd_processor\engine_cmd_processor_bundle.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c -o engine_cmd_processor_test.exe -pthread
```

실행:

```powershell
.\engine_cmd_processor_test.exe
```

현재 테스트 범위:

- `SELECT` binary body 계약 검증
- 같은 테이블 read-read 병렬 실행
- 같은 id 동시 조회
- 같은 PK 동시 INSERT
- 같은 UK 동시 INSERT
- 같은 테이블 read-write 직렬화
- 같은 id read-update 직렬화

판정 기준:

- duplicate PK/UK는 정확히 하나만 성공해야 함
- 최종 row count는 1이어야 함
- 동시 조회는 모두 정상 응답해야 함
- read/write, read/update 경합은 직렬화되어야 함

### 4. 외부 TCP mixed CRUD 부하 테스트

목적:

- 실제 과제 요구사항에 맞게 외부 클라이언트가 TCP로 접속한 상태에서 성능과 무결성을 함께 검증
- SELECT만 몰아넣는 테스트가 아니라 INSERT/UPDATE/DELETE/SELECT를 섞은 랜덤 workload 확인

명령 예시:

```powershell
python scripts\tcp_mixed_workload.py --rows 10000 100000 --request-count 10000 --skip-build-image
python scripts\tcp_mixed_workload.py --rows 1000000 --request-count 10000 --skip-build-image
```

주요 파라미터:

- `--rows`: 초기 테이블 row 수
- `--request-count`: mixed SQL 개수
- `--connections`: TCP 연결 수
- `--concurrency`: 비동기 in-flight 개수
- `--workers`: 서버 worker thread 수
- `--shards`: engine shard 수
- `--queue-capacity`: shard queue 길이
- `--planner-cache`: route plan cache 크기
- `--reuse-existing`: 기존 CSV를 재사용

현재 기본값:

- `connections=16`
- `concurrency=256`
- `workers=32`
- `shards=32`
- `queue-capacity=2048`
- `planner-cache=4096`

## Mixed CRUD workload 구성 원칙

현재 `scripts/tcp_mixed_workload.py`는 아래 조건을 만족하도록 구성되어 있다.

- `SELECT`, `INSERT`, `UPDATE`, `DELETE`가 섞여 있음
- PK exact-match만 쓰지 않음
- `email`, `phone`, `github`, `id range`가 섞여 있음
- duplicate PK/duplicate email/duplicate phone을 의도적으로 포함
- `UPDATE`와 `DELETE`도 중복/경합 상황을 포함

즉 “전부 SELECT *” 혹은 “전부 같은 종류의 쿼리”가 아니라, 실제 DB 사용에 가까운 혼합 부하다.

## 결과 해석 방법

결과 파일:

- `artifacts/tcp_mixed/<dataset>/result.json`
- `artifacts/tcp_mixed/report.md`

핵심 지표:

- `throughput_rps`
- `p95_ms`
- `unexpected_processing_errors`
- `unexpected_statuses`
- `duplicate_email_count`
- `duplicate_phone_count`

정상 판정:

- `unexpected_processing_errors == 0`
- `unexpected_statuses == {}`
- `duplicate_email_count == 0`
- `duplicate_phone_count == 0`

주의:

- `PROCESSING_ERROR`가 있어도 모두 실패는 아니다.
- 의도적으로 넣은 duplicate PK/email/phone 충돌은 `PROCESSING_ERROR`가 정상이다.
- 중요한 것은 예상하지 못한 실패가 없는지와 최종 active state 무결성이 유지되는지다.

## 현재 권장 실행 순서

### 빠른 로컬 점검

```powershell
gcc -O2 -fdiagnostics-color=always -g main.c -o sqlsprocessor.exe
.\engine_cmd_processor_test.exe
python -m py_compile scripts\tcp_mixed_workload.py
```

### 중간 규모 외부 TCP 검증

```powershell
python scripts\tcp_mixed_workload.py --rows 10000 100000 --request-count 10000 --skip-build-image
```

### 대규모 외부 TCP 검증

```powershell
python scripts\tcp_mixed_workload.py --rows 1000000 --request-count 10000 --skip-build-image
```

## timeout 운영 기준

긴 외부 TCP 테스트는 무한 대기하지 않는 것이 중요하다.

현재 권장 기준:

- smoke/문법 검증: 1분 이내
- 1M rows / 10k SQL 외부 TCP: 3분 이내에 종료 여부 확인
- 장기 부하 실험: 별도 로그 폴링으로 진행

운영 원칙:

- 3분 안에 진행 로그가 전혀 없다면 즉시 중단하고 병목 위치 확인
- 종료가 늦으면 warmup, workload, validation 중 어디서 막히는지 먼저 분리
- 외부 TCP 성능을 내부 함수 호출로 대체해서 결론을 내리면 안 됨

## 이번 코드 기준에서 꼭 봐야 할 병목

### DB/worker 쪽

- exact-match가 인덱스를 타는지
- large table에서 eager materialization이 적용되는지
- same-table read/write가 직렬화되는지
- duplicate PK/UK 처리 후 무결성이 유지되는지

### TCP 쪽

- 한 바이트씩 `recv()` 하지 않는지
- JSON parse/serialize 비용이 지나치게 큰지
- connection/inflight 제한이 너무 낮지 않은지
- 외부 mixed workload에서 worker 개선 효과를 TCP front가 다시 잡아먹고 있지 않은지

## 현재 검증된 명령

```powershell
gcc -O2 -fdiagnostics-color=always -g main.c -o sqlsprocessor.exe
gcc -O2 -fdiagnostics-color=always -g -Icmd_processor cmd_processor\engine_cmd_processor_test.c cmd_processor\cmd_processor.c cmd_processor\engine_cmd_processor_bundle.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c -o engine_cmd_processor_test.exe -pthread
.\engine_cmd_processor_test.exe
python -m py_compile scripts\tcp_mixed_workload.py
python scripts\tcp_mixed_workload.py --rows 1000000 --request-count 10000 --skip-build-image
```

이 문서의 목적은 “무슨 테스트를 돌리면 되는가”뿐 아니라, “어떤 테스트 결과로 어떤 결론을 내려야 하는가”까지 팀이 공유할 수 있게 만드는 것이다.
