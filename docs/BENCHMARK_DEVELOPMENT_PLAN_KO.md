# B+ Tree 벤치마크 개발 계획 (결정사항 + 실행안)

## 1. 문서 목적

- 이 문서는 현재까지 합의한 벤치마크 방향과 남은 개발 계획을 한 번에 정리한 개발 기준서입니다.
- SQL 실행기(`sqlsprocessor`)는 다른 팀원이 계속 리팩토링하므로, 본 문서는 **테스트 환경/평가기 구축 범위**에 집중합니다.

## 2. 현재까지 확정한 결정사항

### 2.1 평가 철학

- malloc-lab 방식을 참고하되, 본 프로젝트 목적에 맞춰 가중치를 조정합니다.
- 최종 점수는 `throughput`과 `util`의 균형으로 계산합니다.
- 정확성 실패(기능 테스트 실패)가 발생하면 성능 점수는 무효 처리합니다.
- 과제의 메인이 속도 측정이므로, throughput은 하드 캡을 두지 않고 개선량이 계속 반영되게 설계합니다.

### 2.2 점수 프레임

- 총점 프레임: `60 * throughput + 40 * util`
- throughput은 CRUD 전체를 측정하되 읽기(SELECT)를 가장 높은 비중으로 둡니다.
- 1차 CRUD 가중치(합의안):
  - `SELECT 0.60`
  - `INSERT 0.20`
  - `UPDATE 0.15`
  - `DELETE 0.05`

### 2.3 데이터/도메인 시나리오

- 시나리오: `부트캠프 지원자 관리 시스템`
- 기준 스키마/생성 규칙은 기존 문서 `JUNGLE_DATASET_DESIGN_KO.md`를 따릅니다.
- 대량 테스트 기준은 `1,000,000`건 이상 INSERT를 기본 타깃으로 유지합니다.

### 2.4 측정 원칙

- 성능 벤치와 정확성 벤치를 분리합니다.
- 성능 측정에는 성공하는 쿼리만 포함하고, 제약 위반 케이스는 정확성 테스트 전용으로 분리합니다.
- 벤치 실행은 반드시 **순차(single process)** 로 수행합니다.
  - 같은 CSV/DELTA 파일을 병렬로 접근하면 데이터 오염 가능성이 있습니다.

### 2.5 DELETE 처리 상태

- 현재 DELETE 경로는 수정 중입니다.
- 당분간 DELETE 처리량은 `예상치`를 사용하고, 가중치(0.05)를 낮게 유지해 전체 점수 왜곡을 제한합니다.

## 3. 점수 산정 초안 (v1)

## 3.1 용어

- `T_x`: 측정 throughput(ops/sec)
- `R_x`: 기준 throughput(reference)
- `N_x`: 정규화 점수 `T_x / R_x` (하드 캡 없음)

## 3.2 throughput 점수

- `S_select = 0.60*N_id + 0.30*N_uk + 0.10*N_scan`
- `S_thru = 0.60*S_select + 0.20*N_insert + 0.15*N_update + 0.05*N_delete`

## 3.3 util 점수

- `S_util = util / R_util`
- util 정의(초안):
  - `util = max_live_payload_bytes / peak_heap_requested_bytes`

## 3.4 최종 점수

- `Score = 100 * (0.60*S_thru + 0.40*S_util)`
- `R_x`는 점수 100 근처를 맞추는 기준점이며, 상한선이 아닙니다.
- 구현이 reference를 넘으면 점수도 100을 초과할 수 있습니다.

## 3.5 높은 기준(reference) 권장안

- 개선 여지를 남기기 위해 기준을 다소 높게 설정합니다.
- `R_id = 1,600,000 ops/s`
- `R_uk = 750,000 ops/s`
- `R_scan = 160 ops/s`
- `R_insert = 140,000 ops/s`
- `R_update = 220,000 ops/s`
- `R_delete = 120,000 ops/s` (DELETE 안정화 전 임시치)
- `R_util = 0.82`
- 기준은 "천장값"이 아니라 "성능 기준점"입니다.
- 운영 시에는 점수와 함께 raw throughput(`ops/sec`)를 반드시 함께 공개합니다.

## 4. 구현 범위(테스트 인프라 담당)

### 4.1 데이터/워크로드 생성기

- 목표: 프로필 기반 SQL 자동 생성기를 구현해, 동일 조건에서 반복 가능한 성능/정확성 테스트를 보장
- 구현 대상(초안):
  - 입력: `--profile`, `--seed`, `--preload`, `--ops`, `--output-dir`
  - 출력:
    - `workload_<profile>.sql`
    - `workload_<profile>.meta.json` (seed, row_count, op_count, ratio, 생성 시각)
    - `oracle_<profile>.json` (검증용 예상치; correctness용)
- 공통 조건:
  - seed 고정(재현성)
  - auto-id INSERT 경로 포함 (과제 요구사항 정합)
  - 성능용(valid)과 오류유도용(invalid) 워크로드 분리
  - 같은 테이블 파일 동시 접근 금지(순차 실행 가정)

#### 4.1.1 데이터 형식

- 기본 스키마는 `JUNGLE_DATASET_DESIGN_KO.md`를 사용
- 생성 컬럼 규칙:
  - `id(PK)`: INSERT 시 자동 부여 경로 우선
  - `email(UK)`, `phone(UK)`: 중복 없는 값 생성
  - `name`: 중복 허용(선형 탐색 비교군)
  - `track(NN)`: 빈 값 금지

#### 4.1.2 명령 비율 (score 프로필)

- CRUD 비율:
  - `SELECT 60%`
  - `INSERT 20%`
  - `UPDATE 15%`
  - `DELETE 5%` (현재는 추정치 기반; DELETE 안정화 후 재조정)
- SELECT 내부 비율:
  - `id exact (B+Tree) 60%`
  - `uk exact (email/phone) 30%`
  - `non-index scan(name) 10%`

#### 4.1.3 개수/요청 크기

- `smoke`:
  - preload: `10,000`
  - mixed ops: `20,000`
  - 목적: 로컬 빠른 반복
- `regression`:
  - preload: `100,000`
  - mixed ops: `100,000`
  - 목적: 회귀 + 안정성
- `score`:
  - preload: `1,000,000`
  - mixed ops: `500,000` (초안, 팀 합의로 조정 가능)
  - 목적: 발표용 본 측정

#### 4.1.4 요청 크기(size) 정의

- SQL 시스템에서는 alloc size 대신 아래를 요청 크기로 간주:
  - `데이터 요청 크기`: preload row 수
  - `부하 요청 크기`: mixed operation 수
  - `scan 부담 크기`: non-index SELECT 비율과 대상 테이블 크기
- 성능 비교 시 위 3개를 메타데이터로 함께 기록

### 4.2 자동 평가기(러너)

- 목표: `build -> correctness -> benchmark -> report` 일괄 실행
- 출력:
  - `raw report.json`
  - `human report.md`
  - 최종 점수/세부 점수 표

### 4.3 메모리(util) 계측

- 1단계: 러너/프로세스 레벨 보조 지표 수집
- 2단계: `malloc/calloc/realloc/free` 래퍼 기반 정밀 계측(`BENCH_MEMTRACK`) 적용

## 5. 작업 순서 (실행 플랜)

1. `spec` 고정
- 점수식, reference, fail policy를 문서로 먼저 확정

2. 생성기 작성
- seed/profile 입력으로 SQL 워크로드 생성

3. correctness gate 작성
- 기존 scenario SQL + 추가 엣지 케이스 통합

4. throughput 러너 작성
- 반복 측정(예: 3회) 후 median 채택

5. util 계측 추가
- 계측 ON/OFF 빌드 옵션 분리

6. 리포트 자동화
- README에 붙일 표 자동 생성

7. Makefile 타깃 추가
- 예: `bench-smoke`, `bench-score`, `bench-report`, `bench-clean`

## 6. 운영 규칙

- 벤치 실행 전 클린업:
  - 바이너리/생성 SQL/CSV/DELTA 정리
- 벤치 실행 중 병렬 작업 금지:
  - 같은 테이블 파일 동시 접근 금지
- 점수 산출 시 반드시 버전 정보 기록:
  - 커밋 SHA(또는 태그), reference 버전, seed 값

## 7. 남은 이슈

- DELETE 구현 안정화 후 `R_delete` 재보정
- util 계측 방식 2단계(정밀 계측) 적용 시점 확정
- score 프로필의 요청 수(ops count) 최종 확정

## 8. 발표 연결 포인트

- "왜 이 점수식이 합리적인가?"
  - 읽기 중심 서비스 특성과 과제 요구사항 반영
- "왜 기준을 높게 잡았는가?"
  - 현재 점수 과대평가를 방지하고 개선 여지를 확보하기 위함
- "왜 정확성 게이트를 먼저 두는가?"
  - 빠른 코드보다 **신뢰 가능한 코드**가 우선이라는 기준을 명확히 하기 위함
