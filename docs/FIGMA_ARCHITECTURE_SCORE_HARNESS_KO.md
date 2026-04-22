# Figma Architecture Score Harness

이 문서는 `CmdProcessor / TCP / REPL` 아키텍처를 Figma로 정리할 때 쓰는 **엄격한 수동 채점 기준**이다.
점수 계산은 사람이 넣고, 합산과 상한 규칙은 스크립트가 계산한다.

목표:
- 95점 이상: 발표용/공유용으로 바로 써도 되는 수준
- 90점 이상: 충분히 강한 결과물
- 88점 미만: 재작업 필요

## 1. 기본 원칙

- 채점 대상은 “보기 좋음”이 아니라 **기술 문서로서의 전달력**이다.
- Figma 화면은 코드 경계, ownership, callback 모델, merge-safe 구조를 정확하게 전달해야 한다.
- 예쁘지만 기술적으로 부정확하면 고득점 불가다.
- 정보는 많되, 밀도 때문에 읽기 어려우면 감점한다.

## 2. 필수 섹션

아래 섹션 중 하나라도 빠지면 95점 불가다.

1. 시스템 개요 / hero
2. frontend -> CmdProcessor -> runtime -> engine 흐름
3. ownership / merge-safe boundary
4. 현재 성능/점수 snapshot
5. implementation notes 또는 next step

## 3. 배점표 (총 100점)

### A. Narrative Clarity (15)
- 0-5: 무엇을 설명하는 화면인지 바로 안 보임
- 6-10: 주제는 보이지만 메시지 우선순위가 약함
- 11-13: 제목, 부제, 핵심 메시지가 명확함
- 14-15: 화면 첫 5초 안에 목적과 요지가 바로 이해됨

### B. Architecture Fidelity (20)
- 0-7: 코드/문서와 어긋나는 표현 있음
- 8-14: 큰 흐름은 맞지만 경계가 흐림
- 15-18: callback, queue, runtime, engine, frontend 경계가 정확함
- 19-20: 구현 상태와 한계까지 정확하게 반영됨

### C. Boundary & Ownership Clarity (15)
- 0-5: TCP 팀과 DB 팀 경계가 안 보임
- 6-10: 일부 보이나 merge-safe 설명이 약함
- 11-13: frontend / processor / runtime ownership이 명확함
- 14-15: 어떤 팀이 어디까지만 건드려야 하는지 즉시 보임

### D. Visual Hierarchy (10)
- 0-3: 시선 흐름이 없음
- 4-7: 제목/본문 구분은 있으나 밀도 제어 부족
- 8-9: 큰 제목, section, supporting text 위계가 선명함
- 10: 어디를 먼저 읽어야 하는지 완전히 자연스러움

### E. Layout & Spacing Discipline (10)
- 0-3: 카드 정렬, 간격, 폭 규칙이 흔들림
- 4-7: 대체로 안정적이나 일부 군집/여백 불균형
- 8-9: spacing system이 일관됨
- 10: 카드/그리드/여백이 발표용 수준으로 정돈됨

### F. Typography & Readability (10)
- 0-3: 작은 글씨, 답답한 line-height, 약한 대비
- 4-7: 읽히지만 밀도 대비 최적화 부족
- 8-9: 제목/본문/보조 텍스트 역할이 잘 나뉨
- 10: 긴 설명도 피로감 없이 읽힘

### G. Color & Polish (10)
- 0-3: 색이 목적 없이 많거나 너무 밋밋함
- 4-7: 기본 톤은 맞으나 강조 체계가 부족
- 8-9: accent color와 neutral tone이 기능적으로 쓰임
- 10: polished한 presentation quality

### H. Information Density Control (5)
- 0-1: 비거나 과밀함
- 2-3: 정보량은 맞지만 일부 카드가 답답함
- 4: 대부분 적절함
- 5: 기술 밀도와 여백 균형이 매우 좋음

### I. Presentation Readiness (5)
- 0-1: 내부 메모 수준
- 2-3: 팀 공유용 정도
- 4: 리뷰/발표 가능
- 5: 외부 데모/정리 자료로도 충분

## 4. 강한 감점 규칙

아래 항목은 별도 penalty로 적용한다.

- giant wall-of-text 카드: -3
- 색 강조가 4개 이상 무질서하게 섞임: -2
- 기술적으로 부정확한 화살표/흐름: -5
- ownership 서술이 모순됨: -5
- current implementation 상태와 다른 주장: -5

## 5. 상한 규칙

다음 조건 중 하나라도 만족하면 총점 상한을 적용한다.

- 필수 섹션 누락: 최대 84
- Architecture Fidelity 15 미만: 최대 89
- Boundary & Ownership Clarity 12 미만: 최대 89
- Visual Hierarchy 8 미만: 최대 92
- Typography & Readability 8 미만: 최대 92
- Color & Polish 8 미만: 최대 94

## 6. 95점 조건

95점 이상을 주려면 아래를 모두 만족해야 한다.

- 필수 섹션 5개 전부 존재
- Architecture Fidelity >= 18
- Boundary & Ownership Clarity >= 14
- Visual Hierarchy >= 9
- Layout & Spacing Discipline >= 9
- Typography & Readability >= 9
- Color & Polish >= 9
- penalty 총합 >= -2

## 7. 리뷰 절차

1. 전체 화면을 본다.
2. 5초 안에 목적이 읽히는지 본다.
3. flow/ownership/score/notes 섹션이 모두 있는지 본다.
4. 카드별 밀도와 typography를 본다.
5. 코드/문서와 기술적으로 맞는지 확인한다.
6. JSON score sheet에 점수를 넣고 스크립트로 총점을 계산한다.

