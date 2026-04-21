#include <ctype.h>
#include <string.h>

#include "lexer.h"
#include "parser.h"

static int expect_token(Parser *parser, SqlTokenType type);
static int finish_statement(Parser *parser, int parsed);
static int parse_literal(Parser *parser, char *dest, size_t dest_size);
static int parse_one_condition(Parser *parser, Statement *stmt);
static int parse_where_clause(Parser *parser, Statement *stmt);
static int parse_select(Parser *parser, Statement *stmt);
static int parse_insert(Parser *parser, Statement *stmt);
static int parse_update(Parser *parser, Statement *stmt);
static int parse_delete(Parser *parser, Statement *stmt);
static int parse_select_target_list(Parser *parser, Statement *stmt);
static int parse_table_name(Parser *parser, Statement *stmt);
static int parse_set_clause(Parser *parser, Statement *stmt);
static int is_literal_token(SqlTokenType type);
static void copy_token_text(char *dest, size_t dest_size, const char *src);
static void sync_legacy_where_fields(Statement *stmt);
static const char *find_values_close_paren(const char *open_paren);
static int only_trailing_space(const char *text);

/* 파서가 현재 토큰을 다음 토큰으로 한 칸 이동시킵니다. */
void advance_parser(Parser *parser) {
    parser->current_token = get_next_token(&parser->lexer);
}

/* 기대 토큰 타입이면 한 칸 이동하고 true(1), 아니면 false(0)를 반환합니다. */
static int expect_token(Parser *parser, SqlTokenType type) {
    if (parser->current_token.type != type) return 0;
    advance_parser(parser);
    return 1;
}

static int finish_statement(Parser *parser, int parsed) {
    if (!parsed) return 0;
    return parser->current_token.type == TOKEN_EOF;
}

static int is_literal_token(SqlTokenType type) {
    return type == TOKEN_STRING ||
           type == TOKEN_NUMBER ||
           type == TOKEN_IDENTIFIER;
}

static void copy_token_text(char *dest, size_t dest_size, const char *src) {
    if (!dest || dest_size == 0) return;
    if (!src) {
        dest[0] = '\0';
        return;
    }

    strncpy(dest, src, dest_size - 1);
    dest[dest_size - 1] = '\0';
}

static int parse_literal(Parser *parser, char *dest, size_t dest_size) {
    if (!is_literal_token(parser->current_token.type)) return 0;
    copy_token_text(dest, dest_size, parser->current_token.text);
    advance_parser(parser);
    return 1;
}

static int parse_one_condition(Parser *parser, Statement *stmt) {
    WhereCondition *condition;

    if (stmt->where_count >= MAX_WHERE_CONDITIONS) return 0;
    if (parser->current_token.type != TOKEN_IDENTIFIER) return 0;

    condition = &stmt->where_conditions[stmt->where_count];
    memset(condition, 0, sizeof(*condition));
    copy_token_text(condition->col, sizeof(condition->col), parser->current_token.text);
    advance_parser(parser);

    if (parser->current_token.type == TOKEN_BETWEEN) {
        advance_parser(parser);
        condition->type = WHERE_BETWEEN;
        if (!parse_literal(parser, condition->val, sizeof(condition->val))) return 0;
        if (!expect_token(parser, TOKEN_AND)) return 0;
        if (!parse_literal(parser, condition->end_val, sizeof(condition->end_val))) return 0;
        stmt->where_count++;
        return 1;
    }

    if (!expect_token(parser, TOKEN_EQ)) return 0;
    condition->type = WHERE_EQ;
    if (!parse_literal(parser, condition->val, sizeof(condition->val))) return 0;
    stmt->where_count++;
    return 1;
}

static void sync_legacy_where_fields(Statement *stmt) {
    WhereCondition *first;

    if (stmt->where_count == 0) return;

    first = &stmt->where_conditions[0];
    stmt->where_type = first->type;
    copy_token_text(stmt->where_col, sizeof(stmt->where_col), first->col);
    copy_token_text(stmt->where_val, sizeof(stmt->where_val), first->val);
    copy_token_text(stmt->where_end_val, sizeof(stmt->where_end_val), first->end_val);
}

/* WHERE cond [AND cond ...] 형식을 파싱합니다. BETWEEN 내부 AND는 조건 구분자로 보지 않습니다. */
static int parse_where_clause(Parser *parser, Statement *stmt) {
    if (parser->current_token.type != TOKEN_WHERE) return 1;

    advance_parser(parser);
    if (!parse_one_condition(parser, stmt)) return 0;

    while (parser->current_token.type == TOKEN_AND) {
        advance_parser(parser);
        if (!parse_one_condition(parser, stmt)) return 0;
    }

    sync_legacy_where_fields(stmt);
    return 1;
}

static int parse_select_target_list(Parser *parser, Statement *stmt) {
    if (parser->current_token.type == TOKEN_STAR) {
        stmt->select_all = 1;
        advance_parser(parser);
        return 1;
    }

    if (parser->current_token.type != TOKEN_IDENTIFIER) return 0;

    stmt->select_all = 0;
    while (1) {
        if (stmt->select_col_count >= MAX_COLS) return 0;

        copy_token_text(stmt->select_cols[stmt->select_col_count],
                        sizeof(stmt->select_cols[stmt->select_col_count]),
                        parser->current_token.text);
        stmt->select_col_count++;
        advance_parser(parser);

        if (parser->current_token.type != TOKEN_COMMA) return 1;
        advance_parser(parser);
        if (parser->current_token.type != TOKEN_IDENTIFIER) return 0;
    }
}

static int parse_table_name(Parser *parser, Statement *stmt) {
    if (parser->current_token.type != TOKEN_IDENTIFIER) return 0;
    copy_token_text(stmt->table_name, sizeof(stmt->table_name), parser->current_token.text);
    advance_parser(parser);
    return 1;
}

static int parse_set_clause(Parser *parser, Statement *stmt) {
    if (parser->current_token.type != TOKEN_IDENTIFIER) return 0;
    copy_token_text(stmt->set_col, sizeof(stmt->set_col), parser->current_token.text);
    advance_parser(parser);

    if (!expect_token(parser, TOKEN_EQ)) return 0;
    return parse_literal(parser, stmt->set_val, sizeof(stmt->set_val));
}

static const char *find_values_close_paren(const char *open_paren) {
    const char *cursor = open_paren + 1;
    int in_quote = 0;

    while (*cursor) {
        if (*cursor == '\'') {
            in_quote = !in_quote;
        } else if (*cursor == ')' && !in_quote) {
            return cursor;
        }
        cursor++;
    }

    return NULL;
}

static int only_trailing_space(const char *text) {
    while (*text) {
        if (!isspace((unsigned char)*text)) return 0;
        text++;
    }
    return 1;
}

/* SELECT 구문 파싱: SELECT * 또는 SELECT col1, col2 FROM table [WHERE ...] */
static int parse_select(Parser *parser, Statement *stmt) {
    stmt->type = STMT_SELECT;
    stmt->select_all = 0;
    stmt->select_col_count = 0;
    advance_parser(parser);

    if (!parse_select_target_list(parser, stmt)) return 0;
    if (!expect_token(parser, TOKEN_FROM)) return 0;
    if (!parse_table_name(parser, stmt)) return 0;
    return parse_where_clause(parser, stmt);
}

/* INSERT 구문 파싱: INSERT INTO table VALUES (...) */
static int parse_insert(Parser *parser, Statement *stmt) {
    const char *open_paren;
    const char *close_paren;
    int value_length;

    stmt->type = STMT_INSERT;
    advance_parser(parser);

    if (!expect_token(parser, TOKEN_INTO)) return 0;
    if (!parse_table_name(parser, stmt)) return 0;
    if (!expect_token(parser, TOKEN_VALUES)) return 0;
    if (parser->current_token.type != TOKEN_LPAREN) return 0;

    open_paren = parser->lexer.sql + parser->lexer.pos - 1;
    close_paren = find_values_close_paren(open_paren);
    if (!open_paren || !close_paren || close_paren <= open_paren) return 0;
    if (!only_trailing_space(close_paren + 1)) return 0;

    value_length = (int)(close_paren - open_paren - 1);
    if (value_length >= (int)sizeof(stmt->row_data)) {
        value_length = (int)sizeof(stmt->row_data) - 1;
    }

    strncpy(stmt->row_data, open_paren + 1, value_length);
    stmt->row_data[value_length] = '\0';

    parser->lexer.pos = (int)(close_paren - parser->lexer.sql + 1);
    advance_parser(parser);
    return parser->current_token.type == TOKEN_EOF;
}

/* UPDATE 구문 파싱: UPDATE table SET col = value [WHERE ...] */
static int parse_update(Parser *parser, Statement *stmt) {
    stmt->type = STMT_UPDATE;
    advance_parser(parser);

    if (!parse_table_name(parser, stmt)) return 0;
    if (!expect_token(parser, TOKEN_SET)) return 0;
    if (!parse_set_clause(parser, stmt)) return 0;
    return parse_where_clause(parser, stmt);
}

/* DELETE 구문 파싱: DELETE FROM table [WHERE ...] */
static int parse_delete(Parser *parser, Statement *stmt) {
    stmt->type = STMT_DELETE;
    advance_parser(parser);

    if (!expect_token(parser, TOKEN_FROM)) return 0;
    if (!parse_table_name(parser, stmt)) return 0;
    return parse_where_clause(parser, stmt);
}

/* SQL 한 줄을 Statement 구조체로 파싱합니다. */
int parse_statement(const char *sql, Statement *stmt) {
    Parser parser;

    memset(stmt, 0, sizeof(*stmt));
    init_lexer(&parser.lexer, sql);
    advance_parser(&parser);

    switch (parser.current_token.type) {
        case TOKEN_SELECT:
            return finish_statement(&parser, parse_select(&parser, stmt));
        case TOKEN_INSERT:
            return parse_insert(&parser, stmt);
        case TOKEN_UPDATE:
            return finish_statement(&parser, parse_update(&parser, stmt));
        case TOKEN_DELETE:
            return finish_statement(&parser, parse_delete(&parser, stmt));
        default:
            stmt->type = STMT_UNRECOGNIZED;
            return 0;
    }
}
