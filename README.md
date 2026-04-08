# 🚀 High-Performance C-SQL Processor

본 프로젝트는 C언어로 구현된 고성능 파일 기반 SQL 처리기입니다. 단순한 데이터 저장을 넘어 **메모리 인덱싱, 이진 탐색(Binary Search), 그리고 복합 제약 조건(PK, UK, NN)**을 지원하여 실제 RDBMS와 유사한 데이터 무결성과 성능을 제공합니다.

## ✨ 핵심 차별점 (Core Features)

### 1. 고성능 인덱싱 및 탐색 알고리즘
- **O(log N) 탐색**: 데이터가 10,000개 이상일 때도 성능 저하를 방지하기 위해 **이진 탐색(Binary Search)**을 수행합니다.
- **메모리 인덱싱**: 테이블 로드 시 PK(숫자)와 UK(문자열)를 메모리 배열에 로드하여, 디스크 I/O 없이 즉각적인 중복 검사를 수행합니다.

### 2. 강력한 데이터 무결성 (Constraints)
- **Primary Key (PK)**: 고유한 숫자 식별자. 중복 시 삽입 차단.
- **Unique Key (UK)**: 고유한 문자열(이메일 등). 중복 시 삽입 차단.
- **Not Null (NN)**: 필수 입력 항목(비밀번호 등). 누락 시 삽입 차단.
- **Normal**: 선택 입력 항목. 미입력 시 자동으로 `NULL` 패딩 처리.

### 3. 파일 입출력 최적화 (I/O Optimization)
- **File Handle Caching**: 10,000번의 쿼리 실행 시 10,000번 파일을 열고 닫는 비효율성을 제거했습니다.
- **Persistent Connection**: 프로그램 실행 중 파일 포인터를 유지하여 디스크 부하를 최소화하고 처리 속도를 극대화했습니다.

## 🛠️ 시작하기 (Quick Start)

### 1. 테이블 스키마 설정
프로그램 실행 전, `.csv` 파일의 첫 줄에 아래와 같은 규칙으로 스키마를 정의해야 합니다.

**파일명: `mydb.users.csv`**
```csv
id(PK),email(UK),pwd(NN),name
```

### 2. 컴파일 및 실행
```bash
# 컴파일
gcc sql.c -o sql_processor

# 실행 (인자값으로 SQL 파일 전달)
./sql_processor queries.txt

# 실행 (대화형 모드)
./sql_processor
```

## 📄 쿼리 예시 (`queries.txt`)
```sql
-- 정상 삽입
INSERT INTO mydb.users VALUES (1, 'hong@test.com', 'pwd123', 'Hong');

-- PK 중복 방어 (실패)
INSERT INTO mydb.users VALUES (1, 'kim@test.com', 'pwd456', 'Kim');

-- 필수값(NN) 누락 방어 (실패)
INSERT INTO mydb.users VALUES (2, 'lee@test.com', , 'Lee');

-- 선택적 입력 & NULL 자동 채우기 (성공)
INSERT INTO mydb.users VALUES (3, 'park@test.com', 'pwd789');

-- 데이터 조회
SELECT * FROM mydb.users;
```

## 🧠 기술적 세부 사항 (For Reviewer)
- **Memory Management**: 모든 데이터는 한 줄씩 스트림 처리되어 메모리 사용량이 고정적입니다. (10,000개 데이터 처리 시에도 메모리 점유 극소)
- **Insertion Sort Logic**: 데이터 삽입 시 인덱스 배열의 정렬 상태를 항상 유지하여 별도의 전체 정렬 과정 없이 `bsearch()`를 즉시 사용할 수 있습니다.
- **Robust Parsing**: 따옴표(`'`) 내부의 세미콜론을 구분하고, 여러 줄에 걸친 SQL 문장을 안전하게 조합하는 오토마타 기반 렉서를 구현했습니다.