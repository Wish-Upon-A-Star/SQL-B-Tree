#include <ctype.h>
#include <string.h>

#include "lexer.h"
#include "parser.h"

/* 파서가 현재 토큰을 다음 토큰으로 한 칸 이동시킵니다. */
void advance_parser(Parser *p) {
    p->current_token = get_next_token(&p->lexer);
}

/* 기대 토큰 타입이면 한 칸 이동하고 true(1), 아니면 false(0) 반환합니다. */
static int expect_token(Parser *p, SqlTokenType type) {
    if (p->current_token.type == type) {
        advance_parser(p);
        return 1;
    }
    return 0;
}

static int finish_statement(Parser *p, int parsed) {
    if (!parsed) return 0;
    return p->current_token.type == TOKEN_EOF;
}

/* WHERE col = value 형식을 파싱해 stmt에 where 컬럼/값을 채웁니다. */
static int parse_where_clause(Parser *p, Statement *stmt) {
    if (p->current_token.type != TOKEN_WHERE) return 1;
    advance_parser(p);

    if (p->current_token.type != TOKEN_IDENTIFIER) return 0;
    strncpy(stmt->where_col, p->current_token.text, sizeof(stmt->where_col) - 1);
    advance_parser(p);

    if (!expect_token(p, TOKEN_EQ)) return 0;

    if (p->current_token.type == TOKEN_STRING ||
        p->current_token.type == TOKEN_NUMBER ||
        p->current_token.type == TOKEN_IDENTIFIER) {
        strncpy(stmt->where_val, p->current_token.text, sizeof(stmt->where_val) - 1);
        advance_parser(p);
    } else {
        return 0;
    }

    return 1;
}

static const char *find_values_close_paren(const char *open_paren) {
    const char *cur = open_paren + 1;
    int in_quote = 0;

    while (*cur) {
        if (*cur == '\'') in_quote = !in_quote;
        else if (*cur == ')' && !in_quote) return cur;
        cur++;
    }
    return NULL;
}

static int only_trailing_space(const char *s) {
    while (*s) {
        if (!isspace((unsigned char)*s)) return 0;
        s++;
    }
    return 1;
}

/* SELECT 구문 파싱: SELECT * 또는 SELECT col1, col2 FROM table [WHERE col = value] */
static int parse_select(Parser *p, Statement *stmt) {
    stmt->type = STMT_SELECT;
    stmt->select_all = 0;
    stmt->select_col_count = 0;
    advance_parser(p);

    if (p->current_token.type == TOKEN_STAR) {
        stmt->select_all = 1;
        advance_parser(p);
    } else if (p->current_token.type == TOKEN_IDENTIFIER) {
        stmt->select_all = 0;
        while (1) {
            if (stmt->select_col_count >= MAX_COLS) return 0;
            strncpy(stmt->select_cols[stmt->select_col_count], p->current_token.text, sizeof(stmt->select_cols[0]) - 1);
            stmt->select_cols[stmt->select_col_count][sizeof(stmt->select_cols[0]) - 1] = '\0';
            stmt->select_col_count++;

            advance_parser(p);
            if (p->current_token.type == TOKEN_COMMA) {
                advance_parser(p);
                if (p->current_token.type != TOKEN_IDENTIFIER) return 0;
                continue;
            }
            break;
        }
    } else {
        return 0;
    }

    if (!expect_token(p, TOKEN_FROM)) return 0;

    if (p->current_token.type != TOKEN_IDENTIFIER) return 0;
    strncpy(stmt->table_name, p->current_token.text, sizeof(stmt->table_name) - 1);
    advance_parser(p);

    return parse_where_clause(p, stmt);
}

/* INSERT 구문 파싱: INSERT INTO table VALUES (...) */
static int parse_insert(Parser *p, Statement *stmt) {
    stmt->type = STMT_INSERT;
    advance_parser(p);
    const char *open_paren;
    const char *close_paren;
    int len;

    if (!expect_token(p, TOKEN_INTO)) return 0;

    if (p->current_token.type != TOKEN_IDENTIFIER) return 0;
    strncpy(stmt->table_name, p->current_token.text, sizeof(stmt->table_name) - 1);
    advance_parser(p);

    if (!expect_token(p, TOKEN_VALUES)) return 0;
    if (p->current_token.type != TOKEN_LPAREN) return 0;

    open_paren = p->lexer.sql + p->lexer.pos - 1;
    close_paren = find_values_close_paren(open_paren);
    if (!open_paren || !close_paren || close_paren <= open_paren) return 0;
    if (!only_trailing_space(close_paren + 1)) return 0;

    len = (int)(close_paren - open_paren - 1);
    if (len >= (int)sizeof(stmt->row_data)) len = (int)sizeof(stmt->row_data) - 1;
    strncpy(stmt->row_data, open_paren + 1, len);
    stmt->row_data[len] = '\0';
    p->lexer.pos = (int)(close_paren - p->lexer.sql + 1);
    advance_parser(p);

    return p->current_token.type == TOKEN_EOF;
}

/* UPDATE 구문 파싱: UPDATE table SET col = value [WHERE col = value] */
static int parse_update(Parser *p, Statement *stmt) {
    stmt->type = STMT_UPDATE;
    advance_parser(p);

    if (p->current_token.type != TOKEN_IDENTIFIER) return 0;
    strncpy(stmt->table_name, p->current_token.text, sizeof(stmt->table_name) - 1);
    advance_parser(p);

    if (!expect_token(p, TOKEN_SET)) return 0;

    if (p->current_token.type != TOKEN_IDENTIFIER) return 0;
    strncpy(stmt->set_col, p->current_token.text, sizeof(stmt->set_col) - 1);
    advance_parser(p);

    if (!expect_token(p, TOKEN_EQ)) return 0;

    if (p->current_token.type == TOKEN_STRING ||
        p->current_token.type == TOKEN_NUMBER ||
        p->current_token.type == TOKEN_IDENTIFIER) {
        strncpy(stmt->set_val, p->current_token.text, sizeof(stmt->set_val) - 1);
        advance_parser(p);
    } else return 0;

    return parse_where_clause(p, stmt);
}

/* DELETE 구문 파싱: DELETE FROM table [WHERE col = value] */
static int parse_delete(Parser *p, Statement *stmt) {
    stmt->type = STMT_DELETE;
    advance_parser(p);

    if (!expect_token(p, TOKEN_FROM)) return 0;

    if (p->current_token.type != TOKEN_IDENTIFIER) return 0;
    strncpy(stmt->table_name, p->current_token.text, sizeof(stmt->table_name) - 1);
    advance_parser(p);

    return parse_where_clause(p, stmt);
}

/*
 * SQL 한 줄을 분해해 Statement 구조체로 바꿔 반환합니다.
 * 성공하면 1, 실패하면 0.
 */
int parse_statement(const char *sql, Statement *stmt) {
    memset(stmt, 0, sizeof(Statement));

    Parser p;
    init_lexer(&p.lexer, sql);
    advance_parser(&p);

    switch (p.current_token.type) {
        case TOKEN_SELECT: return finish_statement(&p, parse_select(&p, stmt));
        case TOKEN_INSERT: return parse_insert(&p, stmt);
        case TOKEN_UPDATE: return finish_statement(&p, parse_update(&p, stmt));
        case TOKEN_DELETE: return finish_statement(&p, parse_delete(&p, stmt));
        default:
            stmt->type = STMT_UNRECOGNIZED;
            return 0;
    }
}


