#ifndef SQL_REPL_ENGINE_H
#define SQL_REPL_ENGINE_H

#include "cmd_processor.h"

#include <stddef.h>

typedef struct {
    char *data;
    size_t len;
    size_t capacity;
    int truncated;
} SqlOutputBuffer;

typedef struct {
    CmdStatusCode status;
    int ok;
    int row_count;
    int affected_count;
} SqlEvalResult;

void sql_output_buffer_init(SqlOutputBuffer *buffer,
                            char *data,
                            size_t capacity);

int sql_output_buffer_append(SqlOutputBuffer *buffer,
                             const char *text);

int sql_output_buffer_appendf(SqlOutputBuffer *buffer,
                              const char *fmt,
                              ...);

SqlEvalResult sql_repl_eval(const char *sql,
                            SqlOutputBuffer *output);

#endif
