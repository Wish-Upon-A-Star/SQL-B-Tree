#ifndef MOCK_CMD_PROCESSOR_H
#define MOCK_CMD_PROCESSOR_H

#include "cmd_processor.h"

int mock_cmd_processor_create(const CmdProcessorContext *base_context,
                              CmdProcessor **out_processor);

#endif
