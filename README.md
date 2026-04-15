# SQL-B-Tree

C 기반 SQL 처리기입니다. CSV 파일을 테이블처럼 사용하며 `INSERT`, `SELECT`, `UPDATE`, `DELETE`를 실행합니다.

이번 버전은 메모리 기반 B+ Tree 인덱스를 추가했습니다. 테이블의 `id(PK)` 컬럼은 자동 증가 ID로 사용할 수 있고, `WHERE id = ...` 형태의 SELECT는 B+ Tree 인덱스를 사용합니다. 다른 컬럼 조건은 비교 대상 성능 확인을 위해 선형 탐색을 사용합니다.

현재 메모리 기반 구현의 안전 상한은 10,000,000건입니다. 1,000,000건을 훨씬 넘는 CSV도 로드할 수 있으며, 상한을 넘으면 부분 로드하지 않고 오류로 중단해 UPDATE/DELETE가 원본 일부를 잃지 않게 합니다.

동시에 캐시하는 CSV 테이블은 최대 1개입니다. 다른 테이블을 열면 기존 테이블을 닫고 새 테이블을 로드해 메모리 사용량을 줄입니다.

`UK` 컬럼은 컬럼별 해시 인덱스로 중복을 검사합니다. 대량 INSERT에서도 기존 전체 레코드를 매번 훑지 않고 평균 O(1)에 가깝게 UK 중복 여부를 확인합니다. UK가 없는 테이블에는 UK 해시 인덱스를 만들지 않습니다. UK 인덱스는 문자열을 한 번 더 복사하지 않고 해시와 row index를 저장하며, 해시 충돌이 나면 원본 row를 다시 읽어 정확 비교합니다.

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
.\sqlsprocessor.exe demo_select.sql
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
.\sqlsprocessor.exe demo_select.sql
.\sqlsprocessor.exe demo_bptree.sql
.\sqlsprocessor.exe --benchmark 1000000
```

## Benchmark Output

`--benchmark` now measures three lookup paths after inserting at least 1,000,000 rows through the normal INSERT path.

```powershell
.\sqlsprocessor.exe --benchmark 1000000
```

- `id SELECT using B+ tree`: numeric PK B+ Tree lookup.
- `email(UK) SELECT using B+ tree`: string UK B+ Tree lookup.
- `name SELECT using linear scan`: non-indexed full scan baseline.

Use the ratio lines to compare indexed lookup against linear scan.

```text
linear/id-index average speed ratio: ...
linear/uk-index average speed ratio: ...
```

The generated `bptree_benchmark_users.csv` file is tracked in this repository so the large-table load path can be tested without regenerating data every time. When an existing CSV table is opened, PK and UK indexes are bulk-built from sorted key-row pairs instead of inserting every key into the tree one by one.
