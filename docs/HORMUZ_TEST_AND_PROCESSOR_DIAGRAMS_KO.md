# 호르무즈 시스템 의사결정 다이어그램

이 문서는 [HORMUZ_SYSTEM_PLAN_KO.md](/Users/wiseungcheol/Desktop/WEDNES_code/SQL-B-Tree/docs/HORMUZ_SYSTEM_PLAN_KO.md)를  
`무엇을 선택했고`, `무엇을 바꿀 수 있고`, `바꾸면 어디까지 영향이 가는지` 중심으로 다시 시각화한 문서다.

목표는 두 가지다.

1. `테스트 설계`와 `처리기 설계`를 분리해서 이해한다.
2. 회의 중 결정이 바뀌었을 때 어느 부분을 같이 바꿔야 하는지 빠르게 찾는다.

읽는 법은 단순하다.

- 초록색: 현재 권장안
- 회색: 대안
- 순서도: 실제 실행 흐름
- 결정 맵: 바꿀 수 있는 선택지

## FigJam 바로가기

- [Hormuz Test Strategy](https://www.figma.com/online-whiteboard/create-diagram/eeec213d-c526-4177-b270-fd564c8f2f08?utm_source=other&utm_content=edit_in_figjam&oai_id=&request_id=4d84e3ec-0dbc-484d-bd7b-2c3b2e3823a0)
- [Hormuz Processor Design](https://www.figma.com/online-whiteboard/create-diagram/b4113746-0bdf-421e-9f91-900aa30cde85?utm_source=other&utm_content=edit_in_figjam&oai_id=&request_id=f4be75dc-78d1-4fe7-bcd0-0d5f724905af)
- [Hormuz B+ Tree Simplified](https://www.figma.com/online-whiteboard/create-diagram/9324ef0f-913d-467c-8b6b-84794bd781f5?utm_source=other&utm_content=edit_in_figjam&oai_id=&request_id=3e5bd54d-8aeb-4407-98bb-2986c6c03867)

---

## 1. 테스트 설계 다이어그램

관련 FigJam: [Hormuz Test Strategy](https://www.figma.com/online-whiteboard/create-diagram/eeec213d-c526-4177-b270-fd564c8f2f08?utm_source=other&utm_content=edit_in_figjam&oai_id=&request_id=4d84e3ec-0dbc-484d-bd7b-2c3b2e3823a0)

### 1-1. 지금 테스트 설계의 핵심

현재 테스트 설계는 `모든 데이터셋 평균형`이 아니라  
`총점 중심 + 핵심 데이터셋 게이트` 구조다.

핵심 이유는 아래와 같다.

- 모든 데이터셋의 목적이 다르다.
- 작은 데이터셋은 구조 버그를 잘 잡는다.
- 큰 데이터셋은 성능과 운영 병목을 잘 드러낸다.
- 따라서 같은 비중 평균보다 `역할별 가중치 + 핵심 fail 규칙`이 더 공정하다.

### 1-2. 테스트 결정 맵

![Hormuz Test Strategy](/Users/wiseungcheol/Downloads/Hormuz%20Test%20Strategy.png)

### 1-3. 테스트 실행 순서도

위 이미지가 테스트 흐름 전체를 같이 표현한다.  
왼쪽은 `의사결정 구간`, 오른쪽은 `실행 순서 구간`으로 읽으면 된다.

### 1-4. 테스트에서 바꿀 수 있는 주요 선택지

| 결정 포인트 | 현재 권장 | 대안 | 바꾸면 생기는 영향 |
|---|---|---|---|
| 평가 단위 | 총점 + 핵심 데이터셋 게이트 | 평균형, 총점-only | 채점 공정성 기준이 바뀜 |
| 데이터셋 크기 | 역할별 혼합 크기 | 전부 100만+, 전부 동일 | 실행 시간과 디버깅 난이도 증가 |
| 대용량 세트 수 | 3개만 100만 | 전부 대용량 | 시간과 메모리 부담 상승 |
| 측정 방식 | 중앙값 기반 | 평균 기반 | 노이즈에 더 취약해짐 |
| suspicious ship 정답 | expected ship_id 파일 | 수동 확인 | 자동 채점 불가능해짐 |
| 결과 출력 | 총점 + 세트 리포트 | 총점만 | 어디서 망가졌는지 보기가 어려움 |

### 1-5. 테스트 설계가 바뀌면 같이 바꿔야 하는 것

| 무엇을 바꾸는가 | 같이 바꿔야 하는 것 |
|---|---|
| 모든 데이터셋을 100만 건으로 통일 | generator 시간, runner 실행 시간, 로컬 디버깅 절차 |
| 평균형 채점으로 변경 | scorer 공식, score.md 해석 방식 |
| suspicious ship 기준 변경 | expected 정답 파일, DS5 생성 규칙 |
| range search 제외 | DS7 의미, 성능 30점 세부 기준 |

---

## 2. 처리기 설계 다이어그램

관련 FigJam: [Hormuz Processor Design](https://www.figma.com/online-whiteboard/create-diagram/b4113746-0bdf-421e-9f91-900aa30cde85?utm_source=other&utm_content=edit_in_figjam&oai_id=&request_id=f4be75dc-78d1-4fe7-bcd0-0d5f724905af)

### 2-1. 지금 처리기 설계의 핵심

현재 권장 처리기 구조는 `JOIN 없는 단일 통합 테이블` 방식이다.

왜 이렇게 두었는지 이유는 명확하다.

- 과제 핵심은 `id 기반 인덱스`, `대용량 적재`, `속도 비교`다.
- JOIN은 스토리는 좋아지지만 파서와 실행기 복잡도가 많이 올라간다.
- 대신 여러 출처 정보를 미리 합친 `permits_enriched` 한 테이블로도 스토리를 충분히 살릴 수 있다.
- 의심 선박은 `필드 불일치`와 `위험 점수`로 표현한다.

### 2-2. 처리기 결정 맵

![Hormuz Processor Design](/Users/wiseungcheol/Downloads/Hormuz%20Processor%20Design.png)

### 2-3. 처리기 실행 순서도

위 이미지는 `선택지 맵`과 `최종 실행 경로`를 같이 보여준다.  
왼쪽에서 데이터 모델과 SQL 범위를 고르고, 중간에서 인덱스 타깃과 저장 방식을 정한 뒤, 오른쪽에서 실제 실행 경로와 EXPLAIN 출력으로 연결된다.

### 2-4. 처리기에서 바꿀 수 있는 주요 선택지

| 결정 포인트 | 현재 권장 | 대안 | 바꾸면 생기는 영향 |
|---|---|---|---|
| 데이터 모델 | 단일 통합 테이블 | 다중 테이블 + JOIN | parser, types, executor가 모두 커짐 |
| SQL 범위 | INSERT/SELECT/DELETE/RANGE | JOIN, UPDATE 적극 확대 | 일정 증가, 설명 난이도 상승 |
| exact lookup 인덱스 | id + ship_id | id만, 다중 문자열 인덱스 | 메모리 사용량과 구현량 변화 |
| 인덱스 방식 | B+ tree | hash, sorted array | range search 가능 여부가 바뀜 |
| suspicious 탐지 | 필드 불일치 비교 | JOIN 비교, risk only | 스토리와 구현 복잡도 변화 |
| 실행 추적 | EXPLAIN/path 출력 | 출력 없음 | 질문 대응력 감소 |
| 삭제 처리 | 안전 우선 일관성 복구 | 빠른 삭제 최적화 | 구현 복잡도 상승 |

### 2-5. 인덱스 저장 방식만 따로 보면

| 방식 | 장점 | 단점 | 이번 과제 적합도 |
|---|---|---|---|
| `B+ tree key -> row_index` | exact lookup과 range search를 둘 다 처리 가능 | 구현이 hash보다 복잡 | 가장 적합 |
| `hash index` | exact lookup은 빠름 | range search에 부적합 | 보조 인덱스면 가능 |
| `sorted array + binary search` | 개념 설명이 쉬움 | 삽입 비용이 큼 | 대량 INSERT에는 불리 |

이번 과제는 `range search`를 넣기로 했기 때문에,  
인덱스 저장 방식은 `B+ tree key -> row_index`가 가장 자연스럽다.

### 2-6. 처리기 설계가 바뀌면 같이 바꿔야 하는 것

| 무엇을 바꾸는가 | 같이 바꿔야 하는 것 |
|---|---|
| 단일 테이블에서 JOIN 구조로 변경 | `types.h`, `parser.c`, `executor.c`, 테스트 쿼리, DS5 생성 규칙 |
| ship_id 인덱스를 빼고 id만 유지 | suspicious ship exact lookup 설명, ship_id 조회 성능 기준 |
| hash index로 변경 | range search 설계, DS7 의미, EXPLAIN 문구 |
| EXPLAIN 제거 | 인덱스 사용 검증 20점 채점 방식 |
| suspicious 기준을 risk_score only로 단순화 | 데이터 생성 규칙, DS5 정답 산출 규칙 |

---

## 3. 지금 기준으로 가장 안정적인 조합

현재 문서 기준으로 가장 안정적이고 설명 가능한 조합은 아래다.

- 테스트:
  - `총점 + 핵심 데이터셋 게이트`
  - `역할별 혼합 크기`
  - `워밍업 1회 + 5회 측정 + 중앙값`
  - `총점 + 데이터셋별 리포트`
- 처리기:
  - `permits_enriched` 단일 테이블
  - `INSERT / SELECT / DELETE / RANGE SELECT`
  - `id + ship_id` B+ tree 인덱스
  - `의심 선박은 필드 불일치 비교`
  - `EXPLAIN으로 INDEX_LOOKUP / RANGE_SCAN / FULL_SCAN 표시`

한 줄로 요약하면:

> 지금 가장 좋은 방향은 테스트는 `총점 + 핵심 게이트`, 처리기는 `단일 통합 테이블 + B+ tree + range search + suspicious filter` 조합이다.

---

## 4. 약식 B+ 트리 다이어그램

관련 FigJam: [Hormuz B+ Tree Simplified](https://www.figma.com/online-whiteboard/create-diagram/9324ef0f-913d-467c-8b6b-84794bd781f5?utm_source=other&utm_content=edit_in_figjam&oai_id=&request_id=3e5bd54d-8aeb-4407-98bb-2986c6c03867)

이 섹션은 발표나 Q&A에서 바로 보여줄 수 있는 아주 단순한 트리 그림이다.  
복잡한 구현 세부보다 `어떻게 내려가고`, `왜 범위 검색에 유리한지`를 설명하는 데 초점을 둔다.

### 4-1. 약식 트리 구조

![Hormuz B+ Tree Simplified](/Users/wiseungcheol/Downloads/Hormuz%20B%2B%20Tree%20Simplified.png)

이 그림에서 설명할 핵심은 세 가지다.

- 위쪽 노드는 `방향을 고르는 용도`다.
- 실제 데이터 위치는 리프에 있다.
- 리프끼리 옆으로 연결되어 있어서 범위 검색이 쉽다.

### 4-2. 정확 조회 예시

예: `WHERE ship_id = 72`

이 흐름은 발표에서 이렇게 설명하면 된다.

> 루트에서 시작해서 키 비교로 아래 노드를 고르고, 최종적으로 리프에서 원하는 `ship_id`를 찾는다.

### 4-3. 범위 조회 예시

예: `WHERE ship_id BETWEEN 70 AND 118`

이 흐름은 발표에서 이렇게 설명하면 된다.

> 시작점이 되는 첫 리프만 찾으면, 그 다음부터는 옆으로 연결된 리프를 따라가면서 필요한 범위를 연속으로 읽으면 된다.

### 4-4. 현재 프로젝트에 그대로 연결하면

현재 프로젝트에서는 이 약식 트리를 아래처럼 대응시켜 설명할 수 있다.

- `WHERE id = ?`
  - 루트에서 리프까지 내려가는 `exact lookup`
- `WHERE ship_id = ?`
  - 루트에서 리프까지 내려가는 `exact lookup`
- `WHERE id BETWEEN A AND B`
  - 시작 리프를 찾은 뒤 `leaf next`를 따라가는 `range scan`
- `WHERE is_suspicious = 1`
  - 현재 권장안에서는 인덱스가 아니라 `full scan`

즉, 발표용 핵심 메시지는 이거다.

> 이번 프로젝트에서 B+ 트리는 `정확 조회`와 `범위 조회`에 직접 사용되고, 의심 선박 필터링은 현재는 단순 조건 스캔으로 남겨 두었다.
