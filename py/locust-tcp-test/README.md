# Locust TCP Test

SQL-B-Tree TCP 서버를 Locust로 부하 테스트하는 별도 uv 프로젝트다.

## Setup

```bash
cd py/locust-tcp-test
uv sync
```

## Run

기본값은 worker `1,2,4,8,12,16,24,32`를 돌고, 각 worker마다 `30s warmup + 60s measure + 30s cooldown`을 수행한다. 목표 처리량 기본값은 `50,000 req/s`다.

```bash
uv run python run_locust_tcp_worker_sweep.py
```

빠른 확인용:

```bash
uv run python run_locust_tcp_worker_sweep.py \
  --worker-counts 1,2 \
  --warmup-seconds 3 \
  --measure-seconds 5 \
  --cooldown-seconds 3 \
  --no-plot
```

결과는 repo root 기준 아래에 저장된다.

```text
artifacts/locust_tcp_worker_sweep/report.json
artifacts/locust_tcp_worker_sweep/report.md
artifacts/locust_tcp_worker_sweep/throughput_percentiles.png
```

그래프는 x축이 server worker 수, y축이 요청/초, 선이 `p5/p15/p50/p75/p95` 초당 처리량이다.

## Options

```bash
uv run python run_locust_tcp_worker_sweep.py --help
```

자주 바꾸는 값:

```bash
uv run python run_locust_tcp_worker_sweep.py \
  --worker-counts 1,2,4,8,16,32 \
  --locust-users 64 \
  --pipeline-depth 64 \
  --op sql
```

목표 처리량을 정해놓고 버티는지 보려면 `--target-rps`를 쓴다. 여기서 `--locust-users`는 총 TCP client socket 수다. 구현상 Locust user 1개가 `on_start`에서 TCP socket 1개를 열고, 그 socket에서 `--pipeline-depth`만큼 요청을 pipelining한다.

예를 들어 전체 50,000 req/s를 20개 TCP socket이 나눠 시도하게 하려면 아래처럼 실행한다.

```bash
uv run python run_locust_tcp_worker_sweep.py \
  --worker-counts 1,2,4,8,12,16,24,32 \
  --locust-users 20 \
  --spawn-rate 20 \
  --pipeline-depth 32 \
  --target-rps 50000
```

이 경우 각 TCP socket은 평균 `50000 / 20 = 2500 req/s`를 목표로 pacing한다. 동시 in-flight 요청 상한은 대략 `--locust-users * --pipeline-depth`다. 결과의 `average_rps`, `success_rps_by_second`, `failure_rps_by_second`를 보면 목표 부하를 실제로 유지했는지 확인할 수 있다.

## Multi-Process Loadgen

단일 Locust 프로세스가 먼저 CPU 병목이 되면 `--loadgen-processes`로 부하 생성기를 여러 프로세스로 나눈다. `--loadgen-processes`가 2 이상이면 Locust master 1개와 worker N개를 띄우는 distributed mode로 실행한다. 총 TCP socket 수(`--locust-users`)는 master가 관리하고, 각 worker 결과 JSON은 다시 합산된다.

```bash
uv run python run_locust_tcp_worker_sweep.py \
  --worker-counts 1,2,4,8,16,32 \
  --loadgen-processes 4 \
  --locust-users 20 \
  --spawn-rate 20 \
  --pipeline-depth 32 \
  --target-rps 50000
```

예를 들어 `--locust-users 20 --loadgen-processes 4`이면 총 TCP socket은 20개이고, Locust master가 worker 4개에 대략 5개씩 분산한다. 각 socket은 동일하게 `target_rps / locust_users` 기준으로 pacing한다.

주의: 50,000 req/s도 단일 머신에서 부하 생성기 CPU 병목이 될 수 있다. 이 경우 결과의 `target_achievement_ratio`가 낮게 나오며, 더 정확한 서버 한계를 보려면 Locust 프로세스를 더 늘리거나 여러 머신으로 분산해야 한다.
