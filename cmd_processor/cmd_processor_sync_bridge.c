#include "cmd_processor_sync_bridge.h"

#include "../platform_threads.h"

#include <string.h>

typedef struct {
    db_mutex_t mutex;
    db_cond_t cv;
    CmdResponse *response;
    int done;
} SyncSubmitResult;

static void sync_submit_callback(CmdProcessor *processor,
                                 CmdRequest *request,
                                 CmdResponse *response,
                                 void *user_data) {
    SyncSubmitResult *result = (SyncSubmitResult *)user_data;
    (void)processor;
    (void)request;
    if (!result) return;
    db_mutex_lock(&result->mutex);
    result->response = response;
    result->done = 1;
    db_cond_broadcast(&result->cv);
    db_mutex_unlock(&result->mutex);
}

int cmd_processor_submit_sync(CmdProcessor *processor,
                              CmdRequest *request,
                              CmdResponse **out_response) {
    SyncSubmitResult result;
    int mutex_ready;
    int cv_ready;

    if (out_response) *out_response = NULL;
    if (!processor || !request || !out_response) return -1;

    memset(&result, 0, sizeof(result));
    mutex_ready = db_mutex_init(&result.mutex);
    cv_ready = mutex_ready ? db_cond_init(&result.cv) : 0;
    if (!mutex_ready || !cv_ready) {
        if (cv_ready) db_cond_destroy(&result.cv);
        if (mutex_ready) db_mutex_destroy(&result.mutex);
        return -1;
    }

    if (cmd_processor_submit(processor, request, sync_submit_callback, &result) != 0) {
        db_cond_destroy(&result.cv);
        db_mutex_destroy(&result.mutex);
        return -1;
    }

    db_mutex_lock(&result.mutex);
    while (!result.done) db_cond_wait(&result.cv, &result.mutex);
    db_mutex_unlock(&result.mutex);

    db_cond_destroy(&result.cv);
    db_mutex_destroy(&result.mutex);
    *out_response = result.response;
    return 0;
}
