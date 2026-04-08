# SQLprocessor 발표 대본

## 발표 전 준비

```bash
gcc -fdiagnostics-color=always -g main.c -o sqlsprocessor
```

## 3분 30초 대본

안녕하세요. 4조 발표 시작하겠습니다.  
저희는 C로 SQL 파일을 입력받아 파싱하고, 실행하고, CSV 파일에 저장하는 SQL Processor를 구현했습니다.  
핵심은 단순히 데이터를 읽고 쓰는 것이 아니라, `입력 -> 파싱 -> 실행 -> 저장` 흐름을 직접 만드는 것이었습니다.

README 구조도를 보시면, SQL 파일이 `main.c`로 들어오고, 이후 `lexer.c`, `parser.c`, `executor.c`를 거쳐 최종적으로 CSV 파일과 콘솔 출력으로 이어집니다.  
즉, SQL 문자열을 바로 실행하는 것이 아니라 먼저 구조화한 뒤에 실행하는 방식입니다.

`main.c`에서는 실행 인자나 `scanf`로 파일 경로를 받고, 파일을 `fgetc`로 한 글자씩 읽습니다.  
이렇게 한 이유는 줄바꿈, 여러 SQL 문장, `--` 주석, 그리고 따옴표 안 문자를 안정적으로 구분하기 위해서입니다.  
따옴표 밖에서 `;`를 만나면 하나의 SQL 문장이 끝났다고 보고 파서로 넘기고, 파싱 결과가 `INSERT`, `SELECT`, `UPDATE`, `DELETE`면 실행기로 보냅니다.

필수 요구사항은 `INSERT`와 `SELECT`였지만, 저희는 `UPDATE`, `DELETE`와 `PK`, `UK`, `NN` 제약조건까지 추가로 구현했습니다.

이제 시연을 보겠습니다.

먼저 기준 상태를 맞추기 위해 `demo_reset.sql`을 실행합니다.

```bash
./sqlsprocessor demo_reset.sql
```

그다음 `demo_select.sql`로 전체 조회와 조건 조회를 보여줍니다.

```bash
./sqlsprocessor demo_select.sql
```

이후 `demo_insert.sql`로 새 사용자를 추가하고, 바로 다시 조회해서 실제로 저장됐는지 확인합니다.

```bash
./sqlsprocessor demo_insert.sql
```

다음으로 `demo_insert_error.sql`을 실행합니다.  
여기서는 같은 `id`를 다시 넣어서 PK 중복 에러를 만들고, 다른 `id`지만 같은 `email`을 넣어서 UK 중복 에러도 보여줍니다.  
이 장면으로 저희가 단순 INSERT가 아니라 제약조건까지 검증하고 있다는 점을 설명할 수 있습니다.

```bash
./sqlsprocessor demo_insert_error.sql
```

마지막으로 `demo_update.sql`과 `demo_delete.sql`을 실행해서 수정과 삭제까지 확인합니다.

```bash
./sqlsprocessor demo_update.sql
./sqlsprocessor demo_delete.sql
```

정리하면, 저희 프로젝트는 SQL 문장을 읽고, 구조화하고, 실제 파일 데이터에 반영하는 작은 DB 처리기입니다.  
최소 요구사항을 넘어서 CRUD 전체 흐름과 제약조건 검증까지 구현했고, 정상 흐름뿐 아니라 중복 INSERT 같은 예외 흐름도 함께 테스트했습니다.

이상 발표 마치겠습니다. 감사합니다.
