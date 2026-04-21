#ifndef ENGINE_CMD_PROCESSOR_TEST_SUPPORT_H
#define ENGINE_CMD_PROCESSOR_TEST_SUPPORT_H

#include "cmd_processor.h"

#include "../platform_threads.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#include <windows.h>
static unsigned long long engine_cmd_now_us(void) {
    static LARGE_INTEGER freq;
    LARGE_INTEGER counter;
    if (freq.QuadPart == 0) QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return (unsigned long long)((counter.QuadPart * 1000000ULL) / freq.QuadPart);
}
#else
#include <time.h>
static unsigned long long engine_cmd_now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long)ts.tv_sec * 1000000ULL +
           (unsigned long long)(ts.tv_nsec / 1000ULL);
}
#endif

static int engine_cmd_compare_ull(const void *lhs, const void *rhs) {
    const unsigned long long a = *(const unsigned long long *)lhs;
    const unsigned long long b = *(const unsigned long long *)rhs;
    return (a > b) - (a < b);
}

static double engine_cmd_percentile95_us(const unsigned long long *samples, int count) {
    unsigned long long *copy;
    int index;

    if (!samples || count <= 0) return 0.0;
    copy = (unsigned long long *)malloc((size_t)count * sizeof(*copy));
    if (!copy) return 0.0;
    memcpy(copy, samples, (size_t)count * sizeof(*copy));
    qsort(copy, (size_t)count, sizeof(*copy), engine_cmd_compare_ull);
    index = (count * 95 + 99) / 100 - 1;
    if (index < 0) index = 0;
    if (index >= count) index = count - 1;
    {
        double result = (double)copy[index];
        free(copy);
        return result;
    }
}

typedef struct {
    db_mutex_t mutex;
    db_cond_t cond;
    int done;
    CmdResponse *response;
} EngineCmdSyncWait;

static void engine_cmd_sync_wait_init(EngineCmdSyncWait *wait) {
    memset(wait, 0, sizeof(*wait));
    db_mutex_init(&wait->mutex);
    db_cond_init(&wait->cond);
}

static void engine_cmd_sync_wait_destroy(EngineCmdSyncWait *wait) {
    db_cond_destroy(&wait->cond);
    db_mutex_destroy(&wait->mutex);
}

static void engine_cmd_sync_wait_callback(CmdProcessor *processor,
                                          CmdRequest *request,
                                          CmdResponse *response,
                                          void *user_data) {
    EngineCmdSyncWait *wait = (EngineCmdSyncWait *)user_data;
    (void)processor;
    (void)request;
    db_mutex_lock(&wait->mutex);
    wait->response = response;
    wait->done = 1;
    db_cond_signal(&wait->cond);
    db_mutex_unlock(&wait->mutex);
}

static int engine_cmd_submit_and_wait(CmdProcessor *processor,
                                      CmdRequest *request,
                                      CmdResponse **out_response,
                                      unsigned long long *elapsed_us) {
    EngineCmdSyncWait wait;
    unsigned long long start_us;

    if (out_response) *out_response = NULL;
    if (elapsed_us) *elapsed_us = 0;
    engine_cmd_sync_wait_init(&wait);
    start_us = engine_cmd_now_us();
    if (cmd_processor_submit(processor, request, engine_cmd_sync_wait_callback, &wait) != 0) {
        engine_cmd_sync_wait_destroy(&wait);
        return -1;
    }

    db_mutex_lock(&wait.mutex);
    while (!wait.done) db_cond_wait(&wait.cond, &wait.mutex);
    db_mutex_unlock(&wait.mutex);

    if (elapsed_us) *elapsed_us = engine_cmd_now_us() - start_us;
    if (out_response) *out_response = wait.response;
    engine_cmd_sync_wait_destroy(&wait);
    return 0;
}

#endif
