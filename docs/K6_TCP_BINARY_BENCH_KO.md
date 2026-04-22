# K6 TCP Binary Bench

## 목적

이 문서는 현재 저장소의 custom binary TCP 서버를 `k6`로 외부 부하 테스트하는 방법을 정리한다.

대상:
- 외부 TCP 경계
- custom binary wire protocol
- phase별 경과 시간 기록
- 그래프 포함 HTML 결과물 생성

## 왜 기본 k6가 아니라 xk6-tcp를 쓰는가

현재 서버는 HTTP가 아니라 custom binary TCP protocol을 사용한다.

그래서:
- 기본 `k6`만으로는 바로 쓰기 어렵고
- `k6/x/tcp` 확장을 포함한 custom k6 binary가 필요하다

현재 스크립트는 다음 조합을 기준으로 작성되어 있다.
- `k6`
- `xk6`
- `github.com/NAlexandrov/xk6-tcp`

## 포함된 파일

- `scripts/build_k6_tcp.ps1`
  - `xk6`로 TCP extension 포함 k6 binary를 빌드
- `scripts/k6_tcp_binary_bench.js`
  - custom binary protocol을 직접 encode/decode 하는 k6 테스트 본문
- `scripts/run_k6_tcp_binary_bench.ps1`
  - ping/sql/mixed phase를 순서대로 돌리고 각 phase elapsed time과 결과 파일을 남김
- `scripts/k6_tcp_queries.txt`
  - mixed mode 샘플 SQL 목록

## 빌드

사전 준비:
- Go
- `xk6`

설치 예:

```powershell
go install go.k6.io/xk6/cmd/xk6@latest
```

빌드:

```powershell
.\scripts\build_k6_tcp.ps1
```

결과:
- `tools\k6-tcp.exe`

## 실행

예:

```powershell
.\scripts\run_k6_tcp_binary_bench.ps1 `
  -K6Path .\tools\k6-tcp.exe `
  -Host 127.0.0.1 `
  -Port 9090 `
  -VUs 32 `
  -Duration 30s `
  -Phases ping,sql,mixed
```

## 결과물

실행 결과는 다음 경로에 생성된다.

- `artifacts\k6_tcp\<timestamp>\ping\dashboard.html`
- `artifacts\k6_tcp\<timestamp>\sql\dashboard.html`
- `artifacts\k6_tcp\<timestamp>\mixed\dashboard.html`
- `artifacts\k6_tcp\<timestamp>\phases.json`
- `artifacts\k6_tcp\<timestamp>\phases.txt`

각 phase마다 남는 파일:
- `dashboard.html`
  - 그래프 포함 HTML 결과
- `summary.json`
  - k6 summary export
- `run.log`
  - 콘솔 로그

## 중간 시간 보기

`run_k6_tcp_binary_bench.ps1`는 phase별로:
- 시작 시각
- 종료 시각
- elapsed milliseconds

를 콘솔에 바로 출력한다.

또한 `k6_tcp_binary_bench.js`는 `K6_TCP_LOG_EVERY` 간격으로 진행 로그를 찍는다.

예:

```powershell
$env:K6_TCP_LOG_EVERY = "500"
```

## 그래프 보기

그래프는 두 방식으로 볼 수 있다.

1. 실행 후 HTML 확인
- `dashboard.html` 열기

2. 실행 중 웹 대시보드 사용
- `-OpenDashboard` 사용
- 필요하면 `-DashboardPort 5665` 같이 설정

예:

```powershell
.\scripts\run_k6_tcp_binary_bench.ps1 `
  -K6Path .\tools\k6-tcp.exe `
  -Port 9090 `
  -OpenDashboard `
  -DashboardPort 5665
```

주의:
- 브라우저를 열어둔 상태에선 k6 프로세스 종료가 늦어질 수 있다
- 자동 HTML export만 필요하면 기본값처럼 `DashboardPort=-1`로 두는 편이 안전하다

## 환경 변수

`k6_tcp_binary_bench.js`가 읽는 값:

- `K6_TCP_HOST`
- `K6_TCP_PORT`
- `K6_TCP_MODE`
  - `ping`
  - `sql`
  - `mixed`
- `K6_TCP_SQL`
- `K6_TCP_SQL_FILE`
- `K6_TCP_TIMEOUT_MS`
- `K6_TCP_LOG_EVERY`
- `K6_VUS`
- `K6_DURATION`

## 현재 제약

- 현재 스크립트는 request/response ID, status, ok flag를 우선 검증한다
- SQL 문자열은 ASCII 중심으로 사용하는 것을 전제로 작성되어 있다
- 매우 큰 payload를 보내는 장거리 benchmark보다, 현재 저장소의 TCP protocol smoke/load 용도에 맞춰져 있다

## 권장 사용 순서

1. TCP 서버 기동
2. `build_k6_tcp.ps1`로 `k6-tcp.exe` 생성
3. `run_k6_tcp_binary_bench.ps1`로 ping/sql/mixed 실행
4. `dashboard.html`과 `phases.json`으로 결과 비교
