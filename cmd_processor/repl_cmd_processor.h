#ifndef REPL_CMD_PROCESSOR_H
#define REPL_CMD_PROCESSOR_H

#include "cmd_processor.h"

int repl_cmd_processor_create(const CmdProcessorContext *base_context,
                              CmdProcessor **out_processor);

#endif
