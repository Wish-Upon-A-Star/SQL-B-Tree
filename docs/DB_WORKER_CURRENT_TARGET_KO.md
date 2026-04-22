# DB + Worker Current Target

## 기준 날짜

- 2026-04-22

## 현재 목표

현재 목표는 `DB + worker` 범위 안에서 아래 세 가지를 동시에 만족시키는 것이다.

1. `executor.c`와 worker 실행 경로를 계속 줄여서 읽히는 구조로 만든다.
2. exact-match `UPDATE/DELETE`와 hot read 경로의 불필요한 비용을 줄인다.
3. worker 여러 개가 실제로 같은 hot shard에서도 병렬성을 만들어내게 한다.

## 지금 확인된 큰 문제

최근 작업에서 확인된 핵심 문제는 아래와 같다.

- `executor.c`가 너무 많은 책임을 들고 있어 수정할수록 복잡도가 다시 올라간다.
- same-value `UPDATE`도 실제 write처럼 delta, index, rebuild 비용을 먹고 있었다.
- mutation persistence가 single-row, batch, rollback 분기까지 한 함수에 섞여 있었다.
- worker는 여러 개여도 `worker -> fixed shard queue` 구조라서 특정 shard에 요청이 몰리면 실질적으로 worker 하나처럼 동작할 수 있었다.

즉 현재 목표는 새 fast path를 계속 덧대는 것이 아니라,
운영 경로를 줄이면서 worker 병렬성이 실제로 나오게 만드는 것이다.

## 현재까지 반영한 방향

### DB 경로

- dead fast path 제거
- mutation lookup을 `MutationLookupMode`로 통합
- `UPDATE/DELETE`를 `truncated`와 `exact-or-scan` helper로 분리
- row rebuild를 공통 builder로 통합
- same-value update를 no-op로 처리해서 delta/index/rewrite를 생략
- single-row와 batch mutation persistence를 commit/rollback helper 경계로 분리

### Worker 경로

- 요청은 shard 기준으로 enqueue하되, worker는 더 이상 자기 shard 큐만 기다리지 않음
- worker는 선호 shard를 유지하되 다른 shard 큐도 steal 가능
- hot shard에 요청이 몰려도 idle worker가 같은 큐의 작업을 가져가게 함

## 다음 직접 목표

다음으로 직접 볼 대상은 아래다.

- batch mutation 경로의 남은 중복 메모리 복사 축소
- worker queue/lock/execute 시간 계측 정리
- hot shard read와 same-table read의 실제 처리량 비교

## 완료 판정 기준

아래 조건을 동시에 만족해야 현재 목표가 달성된 것으로 본다.

1. `executor.c`와 worker 경로가 이전보다 읽기 쉬워졌다고 코드 단위로 설명할 수 있다.
2. 병렬 read 테스트와 write correctness 테스트가 계속 통과한다.
3. hot shard에서도 `max_concurrent_executions`가 실제로 증가한다.
4. no-op update와 exact mutation 비용 감소가 테스트로 고정돼 있다.
