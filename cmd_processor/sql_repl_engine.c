#include "sql_repl_engine.h"

#include "../executor.h"
#include "../parser.h"

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

static SqlEvalResult make_eval_result(CmdStatusCode status,
                                      int ok,
                                      int row_count,
                                      int affected_count) {
    SqlEvalResult result;

    result.status = status;
    result.ok = ok;
    result.row_count = row_count;
    result.affected_count = affected_count;
    return result;
}

static const char *skip_utf8_bom_and_space(const char *sql) {
    if (!sql) return "";
    if ((unsigned char)sql[0] == 0xEF &&
        (unsigned char)sql[1] == 0xBB &&
        (unsigned char)sql[2] == 0xBF) {
        sql += 3;
    }

    while (isspace((unsigned char)*sql)) sql++;
    return sql;
}

void sql_output_buffer_init(SqlOutputBuffer *buffer,
                            char *data,
                            size_t capacity) {
    if (!buffer) return;
    buffer->data = data;
    buffer->len = 0;
    buffer->capacity = capacity;
    buffer->truncated = 0;
    if (data && capacity > 0) data[0] = '\0';
}

int sql_output_buffer_append(SqlOutputBuffer *buffer,
                             const char *text) {
    size_t text_len;

    if (!buffer || !buffer->data || buffer->capacity == 0 || !text) return 0;
    text_len = strlen(text);
    if (buffer->len + text_len > buffer->capacity) {
        size_t available = buffer->capacity > buffer->len ? buffer->capacity - buffer->len : 0;
        if (available > 0) {
            memcpy(buffer->data + buffer->len, text, available);
            buffer->len += available;
            buffer->data[buffer->len] = '\0';
        }
        buffer->truncated = 1;
        return 0;
    }

    memcpy(buffer->data + buffer->len, text, text_len);
    buffer->len += text_len;
    buffer->data[buffer->len] = '\0';
    return 1;
}

int sql_output_buffer_appendf(SqlOutputBuffer *buffer,
                              const char *fmt,
                              ...) {
    va_list args;
    int written;
    size_t available;

    if (!buffer || !buffer->data || buffer->capacity == 0 || !fmt) return 0;
    available = buffer->capacity > buffer->len ? buffer->capacity - buffer->len + 1 : 0;
    if (available == 0) {
        buffer->truncated = 1;
        return 0;
    }

    va_start(args, fmt);
    written = vsnprintf(buffer->data + buffer->len, available, fmt, args);
    va_end(args);

    if (written < 0) {
        buffer->truncated = 1;
        return 0;
    }
    if ((size_t)written >= available) {
        buffer->len = buffer->capacity;
        buffer->data[buffer->len] = '\0';
        buffer->truncated = 1;
        return 0;
    }

    buffer->len += (size_t)written;
    return 1;
}

static void append_success_body(SqlOutputBuffer *output,
                                const Statement *stmt,
                                int matched_rows,
                                int affected_rows,
                                long generated_id) {
    if (!output || !stmt) return;

    switch (stmt->type) {
        case STMT_SELECT:
            sql_output_buffer_appendf(output,
                                      "SELECT matched_rows=%d",
                                      matched_rows);
            return;
        case STMT_INSERT:
            sql_output_buffer_appendf(output,
                                      "INSERT affected_rows=%d id=%ld",
                                      affected_rows,
                                      generated_id);
            return;
        case STMT_UPDATE:
            sql_output_buffer_appendf(output,
                                      "UPDATE affected_rows=%d",
                                      affected_rows);
            return;
        case STMT_DELETE:
            sql_output_buffer_appendf(output,
                                      "DELETE affected_rows=%d",
                                      affected_rows);
            return;
        default:
            sql_output_buffer_append(output, "OK");
            return;
    }
}

SqlEvalResult sql_repl_eval(const char *sql,
                            SqlOutputBuffer *output) {
    Statement stmt;
    const char *normalized_sql;
    int matched_rows = 0;
    int affected_rows = 0;
    long generated_id = 0;

    normalized_sql = skip_utf8_bom_and_space(sql);
    if (*normalized_sql == '\0') {
        return make_eval_result(CMD_STATUS_OK, 1, 0, 0);
    }

    memset(&stmt, 0, sizeof(stmt));
    if (!parse_statement(normalized_sql, &stmt)) {
        sql_output_buffer_appendf(output,
                                  "[error] parse failed: %s",
                                  normalized_sql);
        return make_eval_result(CMD_STATUS_PARSE_ERROR, 0, 0, 0);
    }

    if (!executor_execute_statement(&stmt, &matched_rows, &affected_rows, &generated_id)) {
        sql_output_buffer_append(output, "[error] execution failed");
        return make_eval_result(CMD_STATUS_PROCESSING_ERROR, 0, 0, 0);
    }

    append_success_body(output, &stmt, matched_rows, affected_rows, generated_id);
    return make_eval_result(CMD_STATUS_OK, 1, matched_rows, affected_rows);
}
