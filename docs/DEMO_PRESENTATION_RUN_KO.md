# `make demo-presentation-run`

## 한 줄 요약

100만 행 테이블 위에서 SQL 처리 성능과 worker 수에 따른 요청 처리량을 한 번에 보여주는 시연 명령이다.

## 실행

```sh
make demo-presentation-run
```

이 명령은 내부적으로 두 가지를 실행한다.

## 1. Score workload 실행

```sh
make demo-score-run
```

보여주는 것:

- 100만 행이 있는 테이블에서 SQL 20만 개를 실제로 실행한다.
- SQL은 `SELECT 60% / INSERT 20% / UPDATE 15% / DELETE 5%`로 섞여 있다.
- parser, executor, B+Tree index, CSV/delta 저장 경로를 함께 통과한다.
- 30초 안에 끝나는지 확인한다.

## 2. Worker 8/16/32 비교

```sh
make demo-worker-sweep-run
```

보여주는 것:

- 같은 100만 행 테이블에서 PK SELECT 요청 20만 개를 보낸다.
- worker 수를 8, 16, 32로 바꿔가며 처리량을 비교한다.
- CLI에 각 worker 수별 처리량 막대가 바로 출력된다.

발표 멘트:

> “두 번째는 같은 데이터에서 worker 수를 8, 16, 32로 바꿔 요청 처리량을 비교합니다. 이 부분은 SQL 파일 순차 실행이 아니라 EngineCmdProcessor의 worker 경로를 보여주는 시연입니다.”

## CLI에서 봐야 할 것

Score 쪽에서는 아래 항목을 본다.

- `실행 SQL 200000개`
- `실행 시간`
- `처리량`
- `시간 예산`
- `SELECT / INSERT / UPDATE / DELETE 개수`

Worker 쪽에서는 아래 항목을 본다.

- `8 workers`
- `16 workers`
- `32 workers`
- 각 worker 수의 `K req/sec`
- 최고 처리량 worker 수
