# SQL-B-Tree

C 기반 SQL 처리기입니다. CSV 파일을 테이블처럼 사용하며 `INSERT`, `SELECT`, `UPDATE`, `DELETE`를 실행합니다.

이번 버전은 메모리 기반 B+ Tree 인덱스를 추가했습니다. `id(PK)` 컬럼은 자동 증가 ID로 사용할 수 있고, `WHERE id = ...` 형태의 SELECT는 B+ Tree 인덱스를 사용합니다. `email(UK)` 컬럼은 UK 인덱스를 사용하며, 인덱스가 없는 컬럼 조건은 선형 탐색으로 비교합니다.

현재 메모리 기반 구현의 안전 상한은 2,000,000건입니다. CSV가 이 상한을 넘으면 앞쪽 2,000,000건만 `TableCache`에 캐시하고, 이후 row는 CSV에 남겨 둔 채 tail scan fallback으로 처리합니다.

동시에 캐시하는 CSV 테이블은 최대 1개입니다. 다른 테이블을 열면 기존 테이블을 닫고 새 테이블을 로드해 메모리 사용량을 줄입니다.

`UK` 컬럼은 컬럼별 문자열 B+ Tree 인덱스로 중복을 검사합니다. 대량 INSERT에서도 기존 전체 레코드를 매번 훑지 않고 빠르게 UK 중복 여부를 확인합니다. 과제 요구사항에 맞춰 PK와 UK 모두 B+ Tree 구조를 유지하며, UK가 없는 테이블에는 UK 인덱스를 만들지 않습니다.

## 빌드

```bash
make
```

또는

```powershell
gcc -O2 main.c -o sqlsprocessor
```

## SQL 실행

```bash
./sqlsprocessor demo_bptree.sql
```

SQL 파일은 세미콜론(`;`)으로 문장을 구분합니다.

## 기본 샘플

`case_basic_users.csv` 와 `demo_bptree.sql` 는 자동 ID, PK/UK 제약, 기본 SELECT 경로를 빠르게 확인하는 용도입니다.

```sql
SELECT * FROM case_basic_users WHERE id = 4;
SELECT * FROM case_basic_users WHERE name = 'AutoUser';
```

`UPDATE`와 `DELETE`도 `WHERE id = ...` 조건이면 먼저 PK B+ Tree로 대상 row를 찾습니다. 실행 결과에 아래 표시가 나오면 각각 인덱스와 선형 탐색 경로를 뜻합니다.

```text
[index] B+ tree id lookup
[scan] linear scan on column 'name'
```

## B+ Tree Range SELECT

`id(PK)`와 `UK` 범위 조건은 B+ Tree leaf linked list를 사용합니다.

```sql
SELECT * FROM case_basic_users WHERE id BETWEEN 2 AND 4;
SELECT * FROM case_basic_users WHERE email BETWEEN 'a@test.com' AND 'm@test.com';
```

실행 결과에 다음 표시가 나오면 B+ Tree range scan 경로를 사용한 것입니다.

```text
[index] B+ tree id range lookup
[index] UK B+ tree range lookup on column 'email'
```

범위 검색은 시작 key가 들어갈 leaf를 찾은 뒤, leaf의 `next` 포인터를 따라가며 끝 key까지 출력합니다. 문자열 UK 범위는 `strcmp` 기준 사전순입니다.

## 정글 지원자 데이터셋

발표용 대규모 데이터셋은 `정글 지원 마감 직전, 지원자 100만 건이 몰린 접수 시스템` 콘셉트로 구성했습니다.

- 이 저장소에는 100만 건 CSV 원본을 커밋하지 않습니다.
- 필요할 때 `make demo-jungle`, `make generate-jungle`, `make generate-jungle-sql` 이 자동으로 `bptree_benchmark_users.csv` 를 생성합니다.
- 파일: `bptree_benchmark_users.csv`
- 헤더: `id(PK),email(UK),phone(UK),name,track(NN),background,history,pretest,github,status,round`
- 예시 row:

```csv
777777,jungle0777777@apply.kr,010-0077-7777,안류서,game_tech_lab,incumbent,frontend_3y,61,gh_0777777,submitted,2026_spring
```

상세 규칙은 [docs/JUNGLE_DATASET_DESIGN_KO.md](/Users/ran/wednesday/SQL-B-Tree/docs/JUNGLE_DATASET_DESIGN_KO.md) 에 정리했습니다.

### 데이터셋 재생성

```bash
./sqlsprocessor --generate-jungle 1000000
```

또는

```bash
make generate-jungle
```

기본 출력 파일은 `bptree_benchmark_users.csv` 이고, 생성물은 `.gitignore` 로 제외됩니다.

## 정글 데모

100만 건 데이터셋에서 바로 인덱스/스캔 경로를 보여주려면 아래 파일을 실행하면 됩니다. `email(UK)`, `phone(UK)`, `track(NN)` 제약이 실제 컬럼으로 드러나고, 이름은 사람 이름처럼 중복될 수 있게 구성했습니다.

```bash
./sqlsprocessor demo_jungle.sql
```

또는

```bash
make demo-jungle
```

`make demo-jungle` 는 데이터셋이 없으면 먼저 생성한 뒤 데모 SQL을 실행합니다.

## 테스트 시나리오

네 가지 흐름으로 나누는 걸 추천합니다.

- 대규모 읽기 데모: `demo_jungle.sql`
- 반복 가능한 회귀 테스트: `scenario_jungle_regression.sql`
- 범위 조회 + UK 변경 + 재오픈 검증: `scenario_jungle_range_and_replay.sql`
- UPDATE 제약 검증: `scenario_jungle_update_constraints.sql`

대규모 읽기 데모는 100만 건 테이블에서 `id`, `email`, `name` 조회 경로를 비교합니다.

```bash
./sqlsprocessor demo_jungle.sql
```

회귀 테스트는 작은 전용 테이블 [jungle_test_users.csv](/Users/ran/wednesday/SQL-B-Tree/jungle_test_users.csv:1) 를 매번 초기화한 뒤, 아래를 순서대로 검증합니다.

- PK 조회
- email UK 조회
- phone UK 조회
- 비인덱스 컬럼 스캔
- UPDATE 후 재조회
- DELETE 후 재조회
- duplicate PK / duplicate email / duplicate phone / NN 제약

```bash
./sqlsprocessor scenario_jungle_regression.sql
```

범위/재오픈 시나리오는 같은 테스트 테이블에서 아래를 확인합니다.

- PK range SELECT
- email UK range SELECT
- phone UK range SELECT
- UK 기준 UPDATE
- UK 기준 DELETE
- 다른 테이블 접근 후 재오픈 시 delta replay 유지

```bash
./sqlsprocessor scenario_jungle_range_and_replay.sql
```

UPDATE 제약 시나리오는 수정 시점의 방어 로직을 따로 확인합니다.

- duplicate email UPDATE 거부
- duplicate phone UPDATE 거부
- NN 컬럼 빈 문자열 UPDATE 거부
- PK UPDATE 거부
- 실패 후 정상 UPDATE가 계속 가능한지 확인

```bash
./sqlsprocessor scenario_jungle_update_constraints.sql
```

## 대용량 SQL 워크로드

CSV 데이터셋과 별도로 `INSERT / UPDATE / DELETE` 용 SQL 파일도 생성할 수 있습니다.

- 기준 데이터: `bptree_benchmark_users.csv`
- 작업 테이블: [jungle_workload_users.csv](/Users/ran/wednesday/SQL-B-Tree/jungle_workload_users.csv:1)
- 생성 명령:

```bash
python3 scripts/generate_jungle_sql_workloads.py
```

또는

```bash
make generate-jungle-sql
```

`make generate-jungle-sql` 도 기준 CSV가 없으면 먼저 생성합니다.

생성 결과물은 `generated_sql/` 아래에 만들어집니다.

- `jungle_insert_1000000.sql`: 100만 건 개별 INSERT
- `jungle_update_1000000.sql`: 100만 건 개별 UPDATE
- `jungle_delete_1000000.sql`: 100만 건 개별 DELETE

권장 실행 순서는 아래처럼 작업 테이블을 빈 헤더 상태로 둔 뒤 `INSERT -> UPDATE -> DELETE` 입니다.

```bash
./sqlsprocessor generated_sql/jungle_insert_1000000.sql
./sqlsprocessor generated_sql/jungle_update_1000000.sql
./sqlsprocessor generated_sql/jungle_delete_1000000.sql
```

`UPDATE` 는 각 지원자 `id`를 기준으로 `status` 를 한 단계씩 앞으로 진행합니다.
예: `submitted -> pretest_pass`, `pretest_pass -> interview_wait`, `interview_wait -> final_wait`

시나리오 체크포인트용 SQL도 함께 준비했습니다.

- [scenario_jungle_workload_after_insert.sql](/Users/ran/wednesday/SQL-B-Tree/scenario_jungle_workload_after_insert.sql:1)
- [scenario_jungle_workload_after_update.sql](/Users/ran/wednesday/SQL-B-Tree/scenario_jungle_workload_after_update.sql:1)
- [scenario_jungle_workload_after_delete.sql](/Users/ran/wednesday/SQL-B-Tree/scenario_jungle_workload_after_delete.sql:1)

권장 흐름은 아래입니다.

```bash
./sqlsprocessor generated_sql/jungle_insert_1000000.sql
./sqlsprocessor scenario_jungle_workload_after_insert.sql
./sqlsprocessor generated_sql/jungle_update_1000000.sql
./sqlsprocessor scenario_jungle_workload_after_update.sql
./sqlsprocessor generated_sql/jungle_delete_1000000.sql
./sqlsprocessor scenario_jungle_workload_after_delete.sql
```

## 성능 테스트

최소 1,000,000건을 INSERT 경로로 생성하고, 세 가지 조회 경로를 비교합니다.

```bash
./sqlsprocessor --benchmark 1000000
```

또는

```bash
make benchmark
```

`--benchmark` 는 일반 INSERT 경로로 최소 1,000,000건을 넣은 뒤 다음을 측정합니다.

- `id SELECT using B+ tree`: 숫자 PK B+ Tree 조회
- `email(UK) SELECT using B+ tree`: 문자열 UK 인덱스 조회
- `name SELECT using linear scan`: 비인덱스 컬럼 전체 스캔

아래 비율 출력으로 인덱스 조회가 전체 스캔보다 얼마나 빠른지 비교할 수 있습니다.

```text
linear/id-index average speed ratio: ...
linear/uk-index average speed ratio: ...
```

## Memory Cache Limit

This implementation keeps the first 2,000,000 rows in `TableCache.records` and builds B+ Tree indexes for that cached region. If a CSV has more rows, the first uncached row offset is remembered and tail PK offsets are indexed in memory.

- PK/UK SELECT first checks the B+ Tree index. If a PK key is not cached, exact lookup uses the tail PK offset index before falling back to scan.
- Non-indexed SELECT scans cached rows in memory first, then scans only the uncached CSV tail.
- INSERT beyond the memory limit appends to CSV only and keeps the uncached tail scan path active.
- UPDATE/DELETE on over-limit tables rewrites the CSV through a full-file fallback, then reloads the cache and recomputes the uncached offset.
- UPDATE on cached tables updates only the changed UK B+ Tree entry when the SET column is a UK column.
- UPDATE/DELETE with `WHERE id = ...` uses the PK B+ Tree to locate the target row before applying the change.
- `--benchmark` requests above 2,000,000 rows are capped to 2,000,000 rows to avoid memory blowups.

## Append-Only Delta Log

Cached tables with a PK no longer rewrite the whole CSV for every UPDATE or DELETE. The executor keeps B+ Tree indexes in memory and persists row changes to `<table>.delta`.

- UPDATE writes committed `U` records to the delta log after memory and B+ Tree checks succeed.
- DELETE writes committed `D` tombstone records to the delta log after removing only the deleted PK/UK entries from B+ Tree indexes.
- On table open, the CSV is loaded first, then committed delta batches are replayed, then PK/UK B+ Tree indexes are bulk-built from the current rows.
- Incomplete delta batches are ignored because only records between `B` and `E` markers are replayed.
- If the table later crosses the memory cache limit, pending delta changes are compacted back into the CSV before new tail rows are appended.
- Large delta logs are compacted back into the CSV once they cross `DELTA_COMPACT_BYTES`.
- Over-limit tables still use the slower full-file fallback because rows outside the cached prefix are not indexed in memory.

## Stable Slot IDs

The cached prefix no longer treats `record_count` as the number of live rows. It is now the number of allocated slots. Live rows are tracked by `record_active[slot_id]`, and free deleted slots are kept in `free_slots`.

- B+ Tree values store a stable `slot_id`, not a compacting array position.
- DELETE removes matching PK/UK entries from B+ Tree indexes, marks slots inactive, then writes a delta tombstone.
- INSERT first reuses an inactive slot when one exists, then appends the new row to the CSV for persistence.
- SELECT, UPDATE, DELETE, PK range scans, and UK range scans all skip inactive slots.
- CSV rewrite/compaction writes only active slots, but the in-memory slot layout remains stable while the table is open.

This replaces the earlier compact-array DELETE behavior. The practical effect is that a B+ Tree search result does not become stale just because another row was deleted before it in memory.

생성된 `bptree_benchmark_users.csv` 파일은 저장소에 커밋하지 않습니다. 데모나 SQL 워크로드 생성 전에 필요하면 다시 만들 수 있습니다. 기존 CSV 테이블을 열 때는 모든 키를 트리에 하나씩 삽입하지 않고, 정렬된 key-row 목록으로 PK/UK 인덱스를 bulk-build합니다.

## 포함된 명령

```bash
make demo-bptree
make generate-jungle
make demo-jungle
make scenario-jungle-regression
make scenario-jungle-range-and-replay
make scenario-jungle-update-constraints
make benchmark
```
