#include <ctype.h>
#include <string.h>

#include "lexer.h"

/* Lexer 생성: SQL 문자열을 파싱할 기준 문자열과 시작 위치를 초기화합니다. */
void init_lexer(Lexer *l, const char *sql) {
    l->sql = sql;
    l->pos = 0;
    if ((unsigned char)sql[0] == 0xEF &&
        (unsigned char)sql[1] == 0xBB &&
        (unsigned char)sql[2] == 0xBF) {
        l->pos = 3;
    }
}

/*
 * 현재 위치에서 하나의 토큰을 읽어 반환합니다.
 * 문자, 숫자, 문자열, 기호(*,=,(,),;)를 구분합니다.
 */
Token get_next_token(Lexer *l) {
    Token t = { .type = TOKEN_ILLEGAL, .text = "" };

    while (l->sql[l->pos] && isspace((unsigned char)l->sql[l->pos])) {
        l->pos++;
    }

    if (l->sql[l->pos] == '\0') {
        t.type = TOKEN_EOF;
        return t;
    }

    char c = l->sql[l->pos];
    int start_pos = l->pos;

    if (strchr("*,=();", c)) {
        switch (c) {
            case '*': t.type = TOKEN_STAR; break;
            case ',': t.type = TOKEN_COMMA; break;
            case '=': t.type = TOKEN_EQ; break;
            case '(': t.type = TOKEN_LPAREN; break;
            case ')': t.type = TOKEN_RPAREN; break;
            case ';': t.type = TOKEN_SEMICOLON; break;
        }
        t.text[0] = c;
        t.text[1] = '\0';
        l->pos++;
    } else if (c == '\'') {
        l->pos++;
        start_pos = l->pos;
        while (l->sql[l->pos] && l->sql[l->pos] != '\'') {
            l->pos++;
        }
        int len = l->pos - start_pos;
        if (len >= (int)sizeof(t.text)) len = (int)sizeof(t.text) - 1;
        strncpy(t.text, l->sql + start_pos, len);
        t.text[len] = '\0';

        if (l->sql[l->pos] == '\'') l->pos++;
        t.type = TOKEN_STRING;
    } else if (isalpha((unsigned char)c) || c == '_') {
        while (l->sql[l->pos] &&
               (isalnum((unsigned char)l->sql[l->pos]) || strchr("_-.", l->sql[l->pos]))) {
            l->pos++;
        }
        int len = l->pos - start_pos;
        if (len >= (int)sizeof(t.text)) len = (int)sizeof(t.text) - 1;
        strncpy(t.text, l->sql + start_pos, len);
        t.text[len] = '\0';

        char upper_text[256];
        int i = 0;
        for (i = 0; t.text[i]; i++) {
            upper_text[i] = toupper((unsigned char)t.text[i]);
        }
        upper_text[strlen(t.text)] = '\0';

        if (strcmp(upper_text, "SELECT") == 0) t.type = TOKEN_SELECT;
        else if (strcmp(upper_text, "INSERT") == 0) t.type = TOKEN_INSERT;
        else if (strcmp(upper_text, "UPDATE") == 0) t.type = TOKEN_UPDATE;
        else if (strcmp(upper_text, "DELETE") == 0) t.type = TOKEN_DELETE;
        else if (strcmp(upper_text, "FROM") == 0) t.type = TOKEN_FROM;
        else if (strcmp(upper_text, "WHERE") == 0) t.type = TOKEN_WHERE;
        else if (strcmp(upper_text, "SET") == 0) t.type = TOKEN_SET;
        else if (strcmp(upper_text, "INTO") == 0) t.type = TOKEN_INTO;
        else if (strcmp(upper_text, "VALUES") == 0) t.type = TOKEN_VALUES;
        else t.type = TOKEN_IDENTIFIER;
    } else if (isdigit((unsigned char)c)) {
        while (l->sql[l->pos] &&
               (isalnum((unsigned char)l->sql[l->pos]) || strchr("_-.", l->sql[l->pos]))) {
            l->pos++;
        }
        int len = l->pos - start_pos;
        if (len >= (int)sizeof(t.text)) len = (int)sizeof(t.text) - 1;
        strncpy(t.text, l->sql + start_pos, len);
        t.text[len] = '\0';
        t.type = TOKEN_NUMBER;
    }
    return t;
}

