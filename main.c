#include <stdio.h>
#include <string.h>
#include <ctype.h>
#if defined(_WIN32)
#include <windows.h>
#endif

#include "types.h"
#include "parser.h"
#include "executor.h"

static void configure_console_encoding(void) {
#if defined(_WIN32)
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
#endif
}

/* SQL 파일에서 ';'로 구분되는 SQL 문장을 순서대로 실행합니다. */
int main(int argc, char *argv[]) {
    configure_console_encoding();

    char filename[256];

    if (argc >= 2) {
        strncpy(filename, argv[1], 255);
        filename[255] = '\0';
    } else {
        printf("입력 SQL 파일 경로: ");
        if (scanf("%255s", filename) != 1) return 1;
    }

    FILE *f = fopen(filename, "r");
    if (!f) {
        printf("[오류] '%s' 파일을 열 수 없습니다.\n", filename);
        return 1;
    }

    char buf[MAX_SQL_LEN];
    int idx = 0;
    int q = 0;
    int ch;

    while ((ch = fgetc(f)) != EOF) {
        if (ch == '-' && !q) {
            int n = fgetc(f);
            if (n == EOF) {
                // '-'가 파일의 마지막 문자일 때는 주석이 아니므로 그대로 처리
            } else if (n == '-') {
                while ((ch = fgetc(f)) != EOF && ch != '\n');
                continue;
            } else {
                ungetc(n, f);
            }
        }

        if (ch == '\'') q = !q;

        if (ch == ';' && !q) {
            buf[idx] = '\0';
            char *s = buf;

            while (isspace((unsigned char)*s)) s++;

            if (strlen(s) > 0) {
                Statement stmt;
                if (parse_statement(s, &stmt)) {
                    if (stmt.type == STMT_INSERT) execute_insert(&stmt);
                    else if (stmt.type == STMT_SELECT) execute_select(&stmt);
                    else if (stmt.type == STMT_DELETE) execute_delete(&stmt);
                    else if (stmt.type == STMT_UPDATE) execute_update(&stmt);
                } else {
                    printf("[오류] 잘못된 SQL 문장입니다: %s\n", s);
                }
            }
            idx = 0;
        } else if (idx < MAX_SQL_LEN - 1) {
            buf[idx++] = (char)ch;
        }
    }

    fclose(f);
    close_all_tables();

    return 0;
}

/*
 * 일부 IDE/에디터는 "현재 파일만 컴파일" 방식으로 main.c 하나만 빌드합니다.
 * 그 경우에도 바로 실행되도록 구현 파일을 여기서 함께 포함합니다.
 */
#include "lexer.c"
#include "parser.c"
#include "executor.c"
