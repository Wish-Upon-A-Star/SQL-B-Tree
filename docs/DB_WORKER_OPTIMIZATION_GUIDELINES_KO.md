# DB + Worker Optimization Guidelines

## 목적

이 문서는 우리 팀의 최적화 범위를 `DB 엔진 + worker thread`로 고정하고,
앞으로의 구조 변경이 다시 누더기처럼 흐르지 않게 하기 위한 기준이다.

기준 문장:

> TCP를 제외하고도 설명 가능한 개선만 DB/worker 최적화로 인정한다.

## 범위

### 포함

- `executor.c`, `executor.h`
- `types.h`
- `bptree.c`, `bptree.h`
- `cmd_processor/engine_cmd_processor*.c`
- planner, route, lock, worker queue
- snapshot, delta, row cache, page cache, auxiliary index
- DB/worker correctness와 throughput 검증

### 제외

- `tcp_cmd_processor.c` 프로토콜 변경
- TCP wire format
- JSON line protocol 최적화
- socket framing, connection model, client 호환성

## 최적화 원칙

### 1. worker 수 증가보다 worker hold time 감소가 먼저다

`worker_count`를 늘리는 것은 마지막 수단이다.

먼저 줄여야 하는 비용은 아래다.

- queue wait
- lock wait
- row parse / rebuild
- mutation persistence 비용
- index miss 이후 scan fallback 비용

### 2. hot path는 exact-match CRUD다

mixed workload에서 가장 자주 비용이 커지는 경로는 아래다.

- `SELECT ... WHERE id = ...`
- `UPDATE ... WHERE id = ...`
- `DELETE ... WHERE id = ...`
- `UPDATE/DELETE ... WHERE email = ...`
- `UPDATE/DELETE ... WHERE phone = ...`
- `UPDATE/DELETE ... WHERE github = ...`

우선순위는 항상 아래 순서를 따른다.

1. exact lookup
2. single-row mutate
3. delta/index sync 최소화
4. no-op write skip

### 3. fallback은 유지하되 공통 경로만 남긴다

scan fallback은 없애는 대상이 아니라 마지막 일반 경로다.

원칙:

- exact path가 가능하면 fallback으로 내려가지 않는다.
- fallback으로 내려가더라도 공통 builder와 공통 persistence 규칙만 사용한다.
- fallback 전용 특수 케이스를 새로 만들지 않는다.

### 4. same-table 병렬성은 lock 제거가 아니라 lock scope 축소로 만든다

정확성을 깨면서 병렬성을 얻지 않는다.

바른 방향:

- 실제 공유 자원만 짧게 잠근다.
- read/read는 최대한 겹치게 둔다.
- read/write, write/write는 correctness 우선으로 직렬화한다.
- table-level 직렬화가 꼭 필요한 구간만 남긴다.

### 5. worker는 고정 shard 소비자가 아니라 균형 있는 소비자가 되어야 한다

worker가 여러 개여도 특정 shard queue에만 worker 하나가 묶이면 실제 병렬성은 잘 나오지 않는다.

바른 방향:

- 요청 route는 유지하되 worker 소비는 더 유연하게 만든다.
- worker는 선호 shard를 가질 수 있지만 다른 shard 큐도 steal할 수 있어야 한다.
- hot shard가 생겨도 idle worker가 그 큐의 일을 가져올 수 있어야 한다.

### 6. 코드 감소 없는 최적화는 의심한다

좋은 최적화는 아래에 가까워야 한다.

- dead path 제거
- 공통 helper 통합
- no-op skip
- 중복 lookup 제거
- copy, parse, append 횟수 감소

경계해야 할 최적화는 아래다.

- 특수 케이스 fast path 계속 추가
- exact path와 fallback 외 제3, 제4 경로 생성
- 측정 없이 worker/shard 숫자만 늘리기

## 현재 우선순위

1. mutation persistence 단순화
2. hot shard worker 병렬성 확보
3. queue/lock/execute 계측 정리
4. mixed workload에서 exact path hit율 유지
5. `executor.c` 추가 감량

## 검증 기준

### correctness

- 엔진 병렬 테스트 통과
- duplicate PK/UK 동시성 테스트 통과
- same-table read/write, same-id read/update 테스트 통과
- no-op update persistence skip 테스트 통과

### parallelism

- across-table read 병렬 테스트 통과
- same-table read 병렬 테스트 통과
- hot shard read에서도 `max_concurrent_executions` 증가 확인

### maintainability

- dead helper 제거
- exact path와 fallback path 경계가 설명 가능
- worker 소비 모델이 코드상으로 짧고 분명함

## 현재 목표의 한 줄 버전

지금 우리가 노리는 것은 이것이다.

> DB/worker 경로를 계속 줄이면서, exact CRUD와 hot shard read에서 실제 처리 시간을 낮추고 worker 병렬성이 숫자뿐 아니라 동작으로도 보이게 만드는 것
