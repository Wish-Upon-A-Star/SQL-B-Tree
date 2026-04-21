#ifndef CMD_PROCESSOR_SYNC_BRIDGE_H
#define CMD_PROCESSOR_SYNC_BRIDGE_H

#include "cmd_processor.h"

/*
 * Frontend-local helper for consumers that need synchronous waiting on top of
 * the callback-based public CmdProcessor API.
 *
 * This helper is intentionally separate from the core CmdProcessor contract so
 * future frontends such as TCP can use submit(callback) directly without
 * touching CLI-specific waiting code.
 */
int cmd_processor_submit_sync(CmdProcessor *processor,
                              CmdRequest *request,
                              CmdResponse **out_response);

#endif
