# 정글 지원자 데이터셋 설계

## 목적

- `B+ Tree` 인덱스 성능을 발표에서 직관적으로 보여주기 위한 100만 건 규모 데이터셋입니다.
- 주제는 `정글 지원 마감 직전, 지원자 100만 건이 몰린 접수 시스템`입니다.
- `WHERE id = ?` 와 `WHERE email = ?` 는 인덱스 경로, `WHERE name = ?` 는 선형 탐색 경로를 비교합니다.

## 스키마

```csv
id(PK),email(UK),phone(UK),name,track(NN),background,history,pretest,github,status,round
```

- `id(PK)`: 지원자 번호
- `email(UK)`: 유일한 지원 이메일
- `phone(UK)`: 유일한 연락처
- `name`: 발표용으로 읽기 쉬운 지원자 이름
- `track(NN)`: 지원 트랙. 빈 값이 들어가면 안 됩니다.
- `background`: 학생/신입/현직/전환/독학 배경
- `history`: 학생이면 전공+학년, 아니면 이력
- `pretest`: 사전 시험 점수
- `github`: GitHub 계정 또는 `none`
- `status`: 전형 상태
- `round`: 모집 라운드

## 값 생성 규칙

### 1. 이메일

- 형식: `jungle%07d@apply.kr`
- 예: `jungle0777777@apply.kr`

### 2. 이름

- 형식: `성 + 두 음절 이름`
- 예: `김민준`
- 사람 이름처럼 중복될 수 있게 생성합니다. 그래서 `WHERE name = ...` 은 같은 이름의 여러 지원자를 반환할 수 있습니다.

### 3. 연락처

- 형식: `010-%04d-%04d`
- 예: `010-0077-7777`
- `phone(UK)` 컬럼이라 중복되면 안 됩니다.

### 4. 트랙

- 값은 세 가지 중 하나만 사용합니다.
  - `sw_ai_lab`
  - `game_lab`
  - `game_tech_lab`
- `id` 순서에 따라 세 트랙이 반복됩니다.
- `track(NN)` 컬럼이라 빈 값이 들어가면 안 됩니다.

### 5. background

- 지원자 분포를 현실적으로 보이게 하되 학생 비중을 높게 둡니다.
- 비율:
  - `student`: 62%
  - `newgrad`: 12%
  - `incumbent`: 12%
  - `switcher`: 9%
  - `selftaught`: 5%

### 6. history

- 학생이면 학교명 없이 전공과 학년만 기록합니다.
- 학생:
  - 형식: `major_<major>_grade_<grade>`
  - 예: `major_cs_grade_3`
- 졸업자:
  - 형식: `major_<major>_graduate`
  - 예: `major_ai_graduate`
- 현직자:
  - 형식: `<role>_<years>y`
  - 예: `backend_2y`, `game_server_4y`
- 전환자:
  - 형식: `<previous_role>_<years>y`
  - 예: `designer_3y`, `teacher_5y`
- 독학/부트캠프:
  - 형식: `<route>_<months>m`
  - 예: `selftaught_18m`, `bootcamp_12m`

### 7. pretest

- 값 범위: `35` ~ `100`
- `id` 기반 규칙으로 생성해 매번 같은 지원자가 같은 점수를 갖게 합니다.

### 8. github

- 대부분 `gh_<id>` 형식의 값을 갖습니다.
- 일부 학생/전환 지원자는 `none` 으로 생성해 실제 지원서처럼 보이게 합니다.

### 9. status

- `withdrawn`, `rejected`, `submitted`, `pretest_pass`, `interview_wait`, `final_wait`, `final_pass`
- 점수와 일부 `id` 규칙에 따라 결정합니다.

### 10. round

- 항상 `2026_spring`

## 예시 row

```csv
777777,jungle0777777@apply.kr,010-0077-7777,안류서,game_tech_lab,incumbent,frontend_3y,61,gh_0777777,submitted,2026_spring
```

- 각 속성을 실제 컬럼으로 분리해 `WHERE track = ...`, `WHERE status = ...`, `WHERE pretest = ...` 같은 실험도 가능하게 했습니다.
- `email` 과 `phone` 은 각각 UK, `track` 은 NN 제약 검증용 컬럼입니다.
- 값에는 `,` 와 `'` 를 넣지 않아 CSV/SQL 파서와 충돌하지 않게 유지합니다.

## 재생성 방법

```bash
make generate-jungle
```

또는

```bash
./sqlsprocessor --generate-jungle 1000000
```

- 기본 출력 파일은 `bptree_benchmark_users.csv` 입니다.
- 기존 벤치마크 파일을 같은 규칙으로 다시 만들 수 있습니다.

## 발표용 쿼리

```sql
SELECT id, email, phone, name, track, background, history, pretest, github, status, round FROM bptree_benchmark_users WHERE id = 777777;
SELECT id, email, phone, name, track, pretest, status FROM bptree_benchmark_users WHERE email = 'jungle0777777@apply.kr';
SELECT id, email, phone, name, track, pretest, status FROM bptree_benchmark_users WHERE phone = '010-0077-7777';
SELECT id, phone, name, track, history, pretest, status FROM bptree_benchmark_users WHERE name = '김민준';
```

## 발표 포인트

- `지원자 번호` 조회는 당연히 빠르게 찾아야 하므로 B+ Tree 설득력이 높습니다.
- `이메일`과 `연락처`는 각각 유니크 컬럼이라 UK 인덱스 데모에 잘 맞습니다.
- `track(NN)` 으로 빈 값 금지 테스트도 함께 보여줄 수 있습니다.
- `이름`은 사람이 검색하고 싶어 하지만 인덱스를 걸지 않은 비교용 컬럼이라 선형 탐색 대비가 분명합니다.
