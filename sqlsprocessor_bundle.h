#ifndef SQLSPROCESSOR_BUNDLE_H
#define SQLSPROCESSOR_BUNDLE_H

/*
 * 일부 IDE/에디터는 main.c 하나만 직접 컴파일합니다.
 * 그 경로를 유지하기 위해 구현 파일 결합은 이 번들 헤더에서만 담당합니다.
 */
#include "lexer.c"
#include "parser.c"
#include "bptree.c"
#include "executor.c"

#endif
