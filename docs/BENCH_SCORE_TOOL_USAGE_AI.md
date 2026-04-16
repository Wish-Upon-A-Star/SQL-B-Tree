# bench-score 도구 이식 가이드

이 문서는 AI가 다른 C 기반 SQL 처리기에도 같은 점수 도구를 붙일 수 있도록 만든 실행 지침이다.
핵심은 `benchmark_runner.c`가 실행서(`bench_score_exec.conf`)를 읽고, 그 실행서에 적힌 SQL 실행 파일을 기준으로 점수를 내는 것이다.

## 최소 파일

다른 프로젝트에 점수 도구만 옮길 때 필요한 최소 파일은 아래 4개다.

```text
benchmark_runner.c
bench_workload_generator.c
bench_score_exec.conf
docs/BENCH_SCORE_TOOL_USAGE_AI.md
```

`Makefile`은 필수가 아니다. 대상 프로젝트가 자체 빌드 방식을 가지고 있으면 실행서의 `build_cmd`만 맞추면 된다.

## 대상 SQL 처리기가 반드시 지원해야 하는 것

최소 실행 형태:

```text
<sql_executable> <sql_file>
```

즉, SQL 파일 경로를 인자로 받아 실행할 수 있어야 한다.

선택 지원:

```text
<sql_executable> --quiet <sql_file>
<sql_executable> --generate-jungle <rows> <output_csv>
<sql_executable> --benchmark <rows>
```

호환 규칙:

```text
--quiet            없어도 된다. 실패하면 runner가 자동으로 일반 실행으로 재시도한다.
--generate-jungle  없어도 된다. 실패하면 generator가 CSV를 직접 만든다.
--benchmark        현재 점수 계산에는 필요하다. SELECT 지표를 이 출력에서 읽는다.
```

## 실행서 설정

기본 파일은 `bench_score_exec.conf`다.

기본 프로젝트에서는 실행 파일명과 빌드 명령을 비워 두어도 된다. 플랫폼별 기본값은 C 코드 안에 있다.

```text
constraint_mode=pkuk
expected_failure_errors=4
```

다른 SQL 처리기를 채점할 때:

```text
sql_exe=/absolute/path/to/sqlprocessor
generator_exe=./bench_workload_generator
build_cmd=
memtrack_build_cmd=
constraint_mode=pkuk
expected_failure_errors=4
```

`build_cmd`와 `memtrack_build_cmd`를 비워 두면 runner가 대상 SQL 처리기를 다시 빌드하지 않는다.

## PK/UK 표기가 없는 팀 처리

팀에 따라 CSV 헤더가 아래처럼 되어 있을 수 있다.

```text
id,email,phone,name,track,background,history,pretest,github,status,round
```

이 경우 `id(PK)`, `email(UK)`, `phone(UK)`, `track(NN)` 같은 제약 표기가 없으므로, 실행서를 plain 모드로 바꿔야 한다.

```text
constraint_mode=plain
expected_failure_errors=0
```

plain 모드에서 generator는 아래 헤더로 CSV를 만든다.

```text
id,email,phone,name,track,background,history,pretest,github,status,round
```

AI가 다른 팀 코드에 붙일 때 판단해야 할 것:

```text
1. 대상 코드가 id(PK) 같은 헤더를 이해하면 constraint_mode=pkuk를 쓴다.
2. 대상 코드가 id라고만 적힌 컬럼을 쓴다면 constraint_mode=plain을 쓴다.
3. plain 모드에서는 PK/UK/NN 위반 테스트를 강제하지 않는다.
4. 그래도 workload의 WHERE id = ?는 id 컬럼 기준으로 실행되어야 한다.
5. 대상 코드에 B+ Tree 인덱스가 없다면 점수는 낮게 나오는 것이 정상이다.
```

## 빌드

Linux/macOS:

```bash
gcc -O2 benchmark_runner.c -o benchmark_runner
gcc -O2 bench_workload_generator.c -o bench_workload_generator
gcc -O2 main.c -o sqlsprocessor
```

Windows PowerShell:

```powershell
gcc -O2 benchmark_runner.c -o benchmark_runner.exe
gcc -O2 bench_workload_generator.c -o bench_workload_generator.exe
gcc -O2 main.c -o sqlsprocessor.exe
```

## 실행

현재 저장소:

```bash
make bench-score
```

실행서 지정:

```bash
make bench-score BENCH_EXEC_SPEC=bench_score_exec.conf
```

runner 직접 실행:

```bash
./benchmark_runner --exec-spec bench_score_exec.conf --profile score --seed 20260415 --repeat 1 --memtrack
```

Windows:

```powershell
.\benchmark_runner.exe --exec-spec bench_score_exec.conf --profile score --seed 20260415 --repeat 1 --memtrack
```

## 다른 SQL 처리기에서 빠른 확인

1차 smoke:

```bash
./benchmark_runner --exec-spec bench_score_exec.conf --profile smoke --rows 1000 --ops 1000 --repeat 1
```

성공 기준:

```text
Correctness Pass = true
profile=smoke ... spec=bench-v1
```

그 다음 100만 건 점수:

```bash
./benchmark_runner --exec-spec bench_score_exec.conf --profile score --seed 20260415 --repeat 1 --update-rows 1000000 --delete-rows 1000000 --memtrack
```

## 생성 파일

```text
generated_sql/jungle_correctness_success_<profile>.sql
generated_sql/jungle_correctness_failure_<profile>.sql
generated_sql/jungle_insert_<profile>.sql
generated_sql/jungle_update_<profile>.sql
generated_sql/jungle_delete_<profile>.sql
generated_sql/workload_<profile>.sql
artifacts/bench/report.json
artifacts/bench/report.md
```

## AI 작업 체크리스트

다른 프로젝트에 붙일 때 AI는 아래 순서로 진행한다.

```text
1. 최소 파일 4개를 프로젝트 루트에 복사한다.
2. 대상 SQL 실행 파일이 SQL 파일 인자를 받을 수 있는지 확인한다.
3. CSV 헤더가 id(PK) 계열인지 id 계열인지 확인한다.
4. id(PK) 계열이면 constraint_mode=pkuk를 쓴다.
5. id 계열이면 constraint_mode=plain, expected_failure_errors=0을 쓴다.
6. sql_exe를 대상 실행 파일 경로로 맞춘다.
7. 대상 실행 파일을 외부에서 이미 빌드한다면 build_cmd와 memtrack_build_cmd를 비운다.
8. smoke를 먼저 실행한다.
9. correctness가 true인지 확인한다.
10. score profile을 실행한다.
```

## 자주 나는 문제

`--quiet`가 없어서 실패:

```text
괜찮다. runner가 자동으로 --quiet 없이 다시 실행한다.
```

`--generate-jungle`가 없어서 실패:

```text
괜찮다. generator가 내부 CSV 생성기로 대체한다.
```

CSV 헤더가 맞지 않음:

```text
id(PK),email(UK),phone(UK)... 이면 pkuk
id,email,phone... 이면 plain
```

Correctness가 false:

```text
pkuk 모드라면 PK/UK/NN 제약 위반이 에러로 잡히는지 확인한다.
plain 모드라면 expected_failure_errors=0인지 확인한다.
```

점수가 낮음:

```text
WHERE id = ?가 B+ Tree를 타지 않음
WHERE email/phone = ?가 인덱스를 타지 않음
UPDATE/DELETE가 전체 CSV rewrite를 자주 함
컬럼 값을 볼 때마다 CSV row 문자열을 반복 파싱함
대용량에서 full scan fallback이 자주 발생함
빌드가 -O2 없이 됨
Docker/VM 파일 I/O가 느림
```

## 제한

`--quiet`와 `--generate-jungle`은 없어도 돌아가게 만들었다.

하지만 `--benchmark <rows>`는 현재 SELECT 지표를 읽기 위해 필요하다. 대상 SQL 처리기에 이 옵션이 없다면, 같은 형식의 benchmark 출력을 구현하거나 `benchmark_runner.c`의 SELECT 측정 방식을 별도 SQL workload 기반으로 바꿔야 한다.
