#ifndef PARSER_H
#define PARSER_H

#include "types.h"

/* SQL 문자열 한 줄을 Statement로 파싱합니다. */
/* 성공하면 1, 실패하면 0을 반환합니다. */
int parse_statement(const char *sql, Statement *stmt);

#endif
