# SQL-B-Tree

C 기반 SQL 처리기입니다. CSV 파일을 테이블처럼 사용하며 `INSERT`, `SELECT`, `UPDATE`, `DELETE`를 실행합니다.

이번 버전은 메모리 기반 B+ Tree 인덱스를 추가했습니다. 테이블의 `id(PK)` 컬럼은 자동 증가 ID로 사용할 수 있고, `WHERE id = ...` 형태의 SELECT는 B+ Tree 인덱스를 사용합니다. 다른 컬럼 조건은 비교 대상 성능 확인을 위해 선형 탐색을 사용합니다.

현재 메모리 기반 구현의 안전 상한은 2,000,000건입니다. CSV가 이 상한을 넘으면 앞쪽 2,000,000건만 `TableCache`에 캐시하고, 이후 row는 CSV에 남겨 둔 채 tail scan fallback으로 처리합니다.

동시에 캐시하는 CSV 테이블은 최대 1개입니다. 다른 테이블을 열면 기존 테이블을 닫고 새 테이블을 로드해 메모리 사용량을 줄입니다.

`UK` 컬럼은 컬럼별 문자열 B+ Tree 인덱스로 중복을 검사합니다. 과제 요구사항에 맞춰 PK와 UK 모두 B+ Tree 구조를 유지하며, UK가 없는 테이블에는 UK 인덱스를 만들지 않습니다.

## 빌드

```powershell
gcc -O2 main.c -o sqlsprocessor
```

`make`가 있는 환경에서는 기존 Makefile도 사용할 수 있습니다.

```bash
make
```

## SQL 실행

```powershell
.\sqlsprocessor.exe demo_bptree.sql
```

SQL 파일은 세미콜론(`;`)으로 문장을 구분합니다.

## 자동 ID INSERT

테이블 헤더가 다음처럼 `id(PK)`를 포함하면 ID 인덱스 대상이 됩니다.

```csv
id(PK),email(UK),phone(UK),pwd(NN),name
```

`id` 값을 생략하면 다음 ID가 자동으로 부여됩니다.

```sql
INSERT INTO case_basic_users VALUES ('auto1@test.com', '010-5555', 'pw5555', 'AutoUser');
```

기존 방식처럼 ID를 직접 넣는 INSERT도 계속 지원합니다.

```sql
INSERT INTO case_basic_users VALUES (4, 'newuser@test.com', '010-4444', 'pw4444', 'NewUser');
```

## B+ Tree SELECT

ID 조건 검색은 B+ Tree를 사용합니다.

```sql
SELECT * FROM case_basic_users WHERE id = 4;
```

실행 결과에 다음 표시가 나오면 인덱스 경로를 사용한 것입니다.

```text
[index] B+ tree id lookup
```

ID가 아닌 컬럼 조건은 선형 탐색을 사용합니다.

```sql
SELECT * FROM case_basic_users WHERE name = 'AutoUser';
```

`UPDATE`와 `DELETE`도 `WHERE id = ...` 조건이면 B+ Tree로 대상 row를 먼저 찾습니다. 단, DELETE는 row index가 당겨지므로 삭제 후 PK/UK B+ Tree를 다시 빌드합니다.

```text
[scan] linear scan on column 'name'
```

## 성능 테스트

최소 1,000,000건을 생성하고 B+ Tree 검색과 선형 검색 속도를 비교합니다.

```powershell
.\sqlsprocessor.exe --benchmark 1000000
```

벤치마크는 `bptree_benchmark_users.csv`를 만들고 SQL INSERT와 같은 내부 삽입 경로로 100만 건 이상을 넣은 뒤 다음을 비교합니다. 벤치마크 테이블에는 `email(UK)`가 포함되어 UK 해시 중복 검사도 함께 검증됩니다.

- `id` 기준 SELECT: B+ Tree 인덱스 검색
- `name` 기준 SELECT: 선형 탐색

벤치마크 CSV는 `.gitignore`에 등록되어 커밋되지 않습니다.

## 포함된 데모

```powershell
.\sqlsprocessor.exe demo_bptree.sql
.\sqlsprocessor.exe --benchmark 1000000
```

## 벤치마크 출력

`--benchmark`는 일반 INSERT 경로로 최소 1,000,000건을 넣은 뒤 세 가지 조회 경로를 측정합니다.

```powershell
.\sqlsprocessor.exe --benchmark 1000000
```

- `id SELECT using B+ tree`: 숫자 PK B+ Tree 조회입니다.
- `email(UK) SELECT using B+ tree`: 문자열 UK B+ Tree 조회입니다.
- `name SELECT using linear scan`: 인덱스가 없는 컬럼의 전체 스캔 기준값입니다.

아래 비율 출력으로 인덱스 조회가 전체 스캔보다 얼마나 빠른지 비교할 수 있습니다.

```text
linear/id-index average speed ratio: ...
linear/uk-index average speed ratio: ...
```

## Memory Cache Limit

This implementation keeps the first 2,000,000 rows in `TableCache.records` and builds B+ Tree indexes only for that cached region. If a CSV has more rows, the first uncached row offset is remembered.

- PK/UK SELECT first checks the B+ Tree index. If the key is not cached, it scans only the uncached CSV tail.
- Non-indexed SELECT scans cached rows in memory first, then scans only the uncached CSV tail.
- INSERT beyond the memory limit appends to CSV only and keeps the uncached tail scan path active.
- UPDATE/DELETE on over-limit tables rewrites the CSV through a full-file fallback, then reloads the cache and recomputes the uncached offset.
- UPDATE on cached tables skips UK B+ Tree rebuild when the SET column is not a UK column.
- UPDATE/DELETE with `WHERE id = ...` uses the PK B+ Tree to locate the target row before applying the change.
- `--benchmark` requests above 2,000,000 rows are capped to 2,000,000 rows to avoid memory blowups.

생성된 `bptree_benchmark_users.csv` 파일은 저장소에 커밋하지 않습니다. 벤치마크를 실행하면 필요할 때 다시 생성됩니다. 기존 CSV 테이블을 열 때는 모든 키를 트리에 하나씩 삽입하지 않고, 정렬된 key-row 목록으로 PK/UK 인덱스를 bulk-build합니다.
