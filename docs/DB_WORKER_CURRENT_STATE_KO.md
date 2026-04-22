# DB + Worker 현재 상태 정리

## 문서 목적

이 문서는 현재 브랜치에서 DB 엔진과 worker thread 관점으로 무엇이 끝났고, 무엇이 아직 남았는지를 팀 기준으로 정리한 문서다.

특히 다음 질문에 답하도록 작성했다.

- 지금 DB 쪽에서 무엇을 수정했는가
- worker thread 구조는 어느 정도까지 정리되었는가
- 외부 TCP benchmark 기준으로 어디까지 검증되었는가
- TCP 담당자가 이어받을 때 어떤 상태를 전제로 보면 되는가

## 이번 기준에서 실제로 바뀐 핵심

### 1. 엔진 `SELECT` body가 JSON이 아니라 binary rowset이 됨

핵심 파일:

- `cmd_processor/cmd_processor.h`
- `cmd_processor/engine_cmd_processor_runtime.c`

의미:

- worker thread가 `SELECT` 결과를 JSON 문자열로 만들지 않음
- 엔진 내부 응답 비용이 줄어듦
- 추후 TCP가 binary wire format으로 바뀌더라도 엔진은 다시 뜯지 않아도 됨

### 2. read 요청 shard 분산이 개선됨

핵심 파일:

- `cmd_processor/engine_cmd_processor_planner.c`

의미:

- 같은 테이블 read가 한 shard queue에 몰리지 않도록 조정
- planner cache hit 후에도 read shard가 raw SQL 기준으로 다시 계산됨

### 3. same-table read/read가 가능한 쪽으로 executor read path를 정리함

핵심 파일:

- `executor.c`
- `types.h`

의미:

- shared `FILE *`와 page-cache의 위험 구간만 table-local IO lock으로 보호
- `SELECT`는 read lock을 사용
- 같은 테이블 `SELECT`가 완전히 inline serialize되던 상태는 벗어남

### 4. large table startup과 exact-match lookup을 줄임

핵심 파일:

- `executor.c`
- `scripts/tcp_mixed_workload.py`

의미:

- `github` exact-match auxiliary index 추가
- binary index snapshot preload 경로 추가
- large table에서 row materialization 경로 보강

## 동시성 관점에서 지금 보장되는 것

### 현재 테스트로 확인된 것

- 같은 PK로 동시에 INSERT하면 정확히 하나만 성공
- 같은 UK로 동시에 INSERT하면 정확히 하나만 성공
- 같은 id를 여러 스레드가 동시에 SELECT해도 정상 응답
- 같은 테이블 read/write는 직렬화
- 같은 id read/update는 직렬화

### 아직 의미상 남아 있는 것

- non-index 조건이 섞인 큰 UPDATE/DELETE mixed workload의 내부 cost는 여전히 큼
- worker 수를 무작정 늘린다고 throughput이 계속 오르지는 않음
- 외부 TCP 기준 전체 처리량은 아직 JSONL protocol cost 영향을 크게 받음

## worker 구조에 대한 현재 판단

현재는 “worker 개수가 부족하다”보다 “worker가 일을 받기 전후 경계 비용이 크다”가 더 맞다.

실제 관찰:

- `workers=32`, `shards=32`가 현재 기준으로 더 나은 편
- `workers=64`, `shards=64`로 단순 증가시키면 오히려 throughput이 떨어질 수 있음

즉 다음 판단이 중요하다.

- queue 분산이 맞는가
- lock hold time이 길지 않은가
- callback/response/writeback이 worker 이득을 잡아먹지 않는가

## 외부 TCP benchmark 기준 현재 수치

검증 조건:

- dataset: `1,000,000 rows`
- workload: `10,000 mixed CRUD SQL`
- protocol: external TCP

최종 검증 수치:

- throughput: 약 `13.26k rps`
- p95: 약 `641 ms`
- unexpected processing error: `0`
- duplicate email/phone: `0 / 0`

해석:

- DB/worker 최적화는 실제 외부 TCP benchmark에서도 반영됨
- 하지만 목표 `1,000,000 SQL / 60s`에 필요한 `16.67k rps`는 아직 미달

## 왜 아직 목표를 못 넘는가

현재 증거상 남은 큰 비용은 아래 세 축이다.

### 1. TCP JSON line protocol 비용

- parse 비용
- escape/serialize 비용
- text framing 비용

### 2. mixed CRUD mutation hot path 비용

- `UPDATE/DELETE` 단건 exact-match도 내부에서 row 재구성 및 index 동기화 비용이 있음

### 3. queue/lock 경합

- worker를 더 늘릴수록 queue/lock 비용이 올라가는 구간이 있음

## TCP 담당자에게 넘길 때 중요한 사실

- 엔진 `SELECT` body는 이미 binary rowset이다.
- 즉 TCP가 원하면 나중에 `body_hex`를 버리고 framed binary protocol로 가도 엔진은 다시 뜯지 않아도 된다.
- DB/worker 쪽 duplicate race와 mixed concurrency 회귀 테스트는 이미 들어가 있다.
- 외부 TCP 측정 스크립트는 `scripts/tcp_mixed_workload.py`에 정리되어 있다.

## DB 담당 기준 다음 우선순위

1. `UPDATE/DELETE` 단건 indexed mutation hot path 더 줄이기
2. mixed workload에서 operation별 latency breakdown 추가
3. 필요 시 `github` 외 hot non-UK column exact-match 인덱스 검토
4. queue/lock hold time을 계측해서 worker 병목을 수치화

## 결론

현재 브랜치는 “DB/worker가 아직 손도 못 댄 상태”는 아니다.

반대로 말하면:

- worker pool은 이미 실제 실행 경로로 들어와 있고
- DB 응답 포맷도 엔진 관점에서는 binary화가 되었고
- 대형 mixed CRUD benchmark를 외부 TCP 기준으로 다시 재볼 수 있는 상태다

즉 main에 올릴 가치가 있는 중간 상태가 아니라, 팀 기준으로 계속 이어서 작업할 수 있는 기준선이다.
