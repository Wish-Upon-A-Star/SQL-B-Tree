# Bench Score Tool Usage for AI Agents

This document explains how to use `benchmark_runner.c` and
`bench_workload_generator.c` in this repository or in another C SQL processor
project. It is intentionally written in simple ASCII English so that both
humans and AI agents can read it reliably on Windows, macOS, Linux, and inside
Docker.

## Goal

The bench score tool runs the same style of workload as `make bench-score`.

It does the following:

1. Prepares a large jungle CSV dataset.
2. Generates SQL workload files.
3. Runs correctness checks.
4. Runs SELECT benchmark, INSERT workload, UPDATE workload, and DELETE workload.
5. Writes benchmark reports.
6. Prints a score table to stdout.

## Required files

Minimum files:

```text
benchmark_runner.c
bench_workload_generator.c
```

Usually useful files in this project:

```text
Makefile
main.c
```

If the target SQL processor is already built, `main.c` and `Makefile` are not
strictly required. In that case, set `SQLSPROCESSOR_EXE` to the target executable
path.

## Target SQL executable contract

The target SQL executable must support this minimum command form:

```text
<sql_executable> <sql_file>
```

In other words, it must be able to execute a SQL file path passed as a command
line argument.

The following options are optional but useful:

```text
<sql_executable> --quiet <sql_file>
<sql_executable> --generate-jungle <rows> <output_csv>
<sql_executable> --benchmark <rows>
```

Compatibility notes:

```text
--quiet            Optional. The runner retries without it if it fails.
--generate-jungle  Optional. The generator creates the CSV internally if it fails.
--benchmark        Required for the current score runner to parse SELECT metrics.
```

## Fallback behavior

### SQL file execution with optional --quiet

`benchmark_runner.c` first tries:

```text
<sql_executable> --quiet generated_sql/some_workload.sql
```

If that command exits with a non-zero status, it retries:

```text
<sql_executable> generated_sql/some_workload.sql
```

This means another SQL processor does not need to implement `--quiet` as long as
it can run a SQL file directly.

### Jungle CSV generation with optional --generate-jungle

`bench_workload_generator.c` first tries:

```text
<sql_executable> --generate-jungle <rows> jungle_benchmark_users.csv
```

If that command fails, or if the generated CSV does not contain enough rows, the
generator creates `jungle_benchmark_users.csv` internally.

Internal CSV header:

```text
id(PK),email(UK),phone(UK),name,track(NN),background,history,pretest,github,status,round
```

This means another SQL processor does not need to implement `--generate-jungle`
for workload generation to work.

## Build commands

### Linux or macOS

```bash
gcc -O2 benchmark_runner.c -o benchmark_runner
gcc -O2 bench_workload_generator.c -o bench_workload_generator
gcc -O2 main.c -o sqlsprocessor
```

### Windows PowerShell

```powershell
gcc -O2 benchmark_runner.c -o benchmark_runner.exe
gcc -O2 bench_workload_generator.c -o bench_workload_generator.exe
gcc -O2 main.c -o sqlsprocessor.exe
```

## Run commands

### Current repository with Makefile

```bash
make bench-score
```

Current Makefile behavior is equivalent to:

```bash
./benchmark_runner --profile score --seed 20260415 --repeat 1
```

### Run the runner directly

Linux or macOS:

```bash
./benchmark_runner --profile score --seed 20260415 --repeat 1
```

Windows PowerShell:

```powershell
.\benchmark_runner.exe --profile score --seed 20260415 --repeat 1
```

## Use another SQL processor executable

Set `SQLSPROCESSOR_EXE`.

Linux or macOS:

```bash
export SQLSPROCESSOR_EXE=/absolute/path/to/your_sql_processor
./benchmark_runner --profile smoke --rows 1000 --ops 1000 --repeat 1
```

Windows PowerShell:

```powershell
$env:SQLSPROCESSOR_EXE="C:\absolute\path\to\your_sql_processor.exe"
.\benchmark_runner.exe --profile smoke --rows 1000 --ops 1000 --repeat 1
```

Expected compatibility:

```text
If target supports --quiet:
  runner uses quiet execution.

If target does not support --quiet:
  runner retries normal SQL file execution.

If target supports --generate-jungle:
  generator uses target-generated CSV.

If target does not support --generate-jungle:
  generator creates a schema-compatible CSV internally.
```

## Runner options

`benchmark_runner` supports:

```text
--profile smoke|regression|score
--seed <number>
--repeat <number>
--rows <number>
--preload <number>
--ops <number>
--memtrack
--report-only
```

`--rows` and `--preload` both set the preload row count.

Default profile sizes:

```text
smoke       rows=10000    ops=20000
regression  rows=100000   ops=100000
score       rows=1000000  ops=500000
```

## Workload generator options

`bench_workload_generator` supports:

```text
--profile smoke|regression|score
--seed <number>
--rows <number>
--preload <number>
--ops <number>
--output-dir <directory>
```

The runner normally invokes the generator automatically. Call the generator
directly only when debugging generated SQL files.

## Quick smoke test

Run this before a large score test.

Linux or macOS:

```bash
./benchmark_runner --profile smoke --rows 1000 --ops 1000 --repeat 1
```

Windows PowerShell:

```powershell
.\benchmark_runner.exe --profile smoke --rows 1000 --ops 1000 --repeat 1
```

Expected success signal:

```text
Correctness Pass = true
profile=smoke ... spec=bench-v1
```

## Full score test

Linux or macOS:

```bash
./benchmark_runner --profile score --seed 20260415 --repeat 1
```

Windows PowerShell:

```powershell
.\benchmark_runner.exe --profile score --seed 20260415 --repeat 1
```

With memory tracking:

```bash
./benchmark_runner --profile score --seed 20260415 --repeat 1 --memtrack
```

## Generated files

SQL workload files:

```text
generated_sql/jungle_correctness_success_<profile>.sql
generated_sql/jungle_correctness_failure_<profile>.sql
generated_sql/jungle_insert_<profile>.sql
generated_sql/jungle_update_<profile>.sql
generated_sql/jungle_delete_<profile>.sql
generated_sql/workload_<profile>.sql
generated_sql/workload_<profile>.meta.json
generated_sql/oracle_<profile>.json
```

Report files:

```text
artifacts/bench/report.json
artifacts/bench/report.md
```

Temporary or persistent table files:

```text
jungle_benchmark_users.csv
jungle_workload_users.csv
jungle_workload_users.idx
jungle_workload_users.delta
```

## AI agent checklist

When attaching this tool to another SQL C project, do this:

```text
1. Copy benchmark_runner.c and bench_workload_generator.c to the project root.
2. Build both files with -O2.
3. Build the target SQL processor.
4. Set SQLSPROCESSOR_EXE if the executable is not named sqlsprocessor.
5. Run a small smoke test first.
6. Check that correctness is true.
7. Run the score profile.
8. Read artifacts/bench/report.md and the stdout score table.
```

## Troubleshooting

### The target does not support --quiet

This is allowed. The runner retries without `--quiet`.

If both attempts fail, the target likely cannot execute a SQL file passed as a
plain command line argument.

### The target does not support --generate-jungle

This is allowed. The generator prints a notice and creates the source CSV
internally.

Expected notice:

```text
[notice] target SQL executable does not provide compatible --generate-jungle; generating source CSV internally.
```

### Correctness is false

Check the target SQL processor behavior:

```text
PK duplicate rejection
UK duplicate rejection
NN empty value rejection
DELETE then INSERT with the same key
UPDATE then SELECT
SELECT WHERE id = ?
SELECT WHERE email = ?
SELECT WHERE phone = ?
```

### Score is very low

Common causes:

```text
SELECT WHERE id = ? does not use a B+ Tree.
SELECT WHERE email/phone = ? does not use a UK index.
UPDATE/DELETE rewrites the whole CSV too often.
The code reparses CSV rows for every column access.
Large tables fall back to full CSV scans.
The executable was built without -O2.
Docker or VM file I/O is slow.
```

### Windows cannot find the executable

Default Windows paths:

```text
.\sqlsprocessor.exe
.\benchmark_runner.exe
.\bench_workload_generator.exe
```

If the SQL executable has another name:

```powershell
$env:SQLSPROCESSOR_EXE=".\your_sql.exe"
```

### Linux or macOS cannot find the executable

Default Unix paths:

```text
./sqlsprocessor
./benchmark_runner
./bench_workload_generator
```

If the SQL executable has another name:

```bash
export SQLSPROCESSOR_EXE=./your_sql
```

## Important limitation

`--quiet` and `--generate-jungle` are optional because this tool has fallback
paths.

`--benchmark <rows>` is different. The current score runner expects benchmark
output from the target SQL executable in order to calculate SELECT throughput.
If another SQL processor does not support `--benchmark <rows>`, either implement
that option or adjust `benchmark_runner.c` to collect SELECT metrics another way.

