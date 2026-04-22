#include "cmd_processor/mock_cmd_processor.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

typedef struct {
    uint64_t completed;
} BenchState;

static uint64_t monotonic_us(void) {
    struct timespec ts;

    timespec_get(&ts, TIME_UTC);
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

static void release_callback(CmdProcessor *processor,
                             CmdRequest *request,
                             CmdResponse *response,
                             void *user_data) {
    BenchState *state = (BenchState *)user_data;

    if (response) cmd_processor_release_response(processor, response);
    if (request) cmd_processor_release_request(processor, request);
    state->completed++;
}

static int run_case(CmdProcessor *processor,
                    const char *label,
                    int count,
                    int is_sql) {
    BenchState state;
    uint64_t start_us;
    uint64_t end_us;
    int i;

    state.completed = 0;
    start_us = monotonic_us();
    for (i = 0; i < count; i++) {
        CmdRequest *request = NULL;
        char id[64];

        if (cmd_processor_acquire_request(processor, &request) != 0) return 1;
        snprintf(id, sizeof(id), "req-%d", i);
        if (is_sql) {
            if (cmd_processor_set_sql_request(processor,
                                              request,
                                              id,
                                              "SELECT * FROM case_basic_users WHERE id = 2;") != CMD_STATUS_OK) {
                return 2;
            }
        } else {
            if (cmd_processor_set_ping_request(processor, request, id) != CMD_STATUS_OK) {
                return 3;
            }
        }
        if (cmd_processor_submit(processor, request, release_callback, &state) != 0) {
            return 4;
        }
    }
    end_us = monotonic_us();

    printf("[pre-worker] case=%s requests=%d elapsed_ms=%.2f rps=%.2f\n",
           label,
           count,
           (double)(end_us - start_us) / 1000.0,
           (double)count * 1000000.0 / (double)(end_us - start_us));
    return state.completed == (uint64_t)count ? 0 : 5;
}

int main(int argc, char **argv) {
    CmdProcessor *processor = NULL;
    int count = 1000000;
    int rc = 1;

    if (argc >= 2) count = atoi(argv[1]);
    if (count <= 0) count = 1000000;

    if (mock_cmd_processor_create(NULL, &processor) != 0) return 10;
    if (run_case(processor, "ping", count, 0) != 0) goto cleanup;
    if (run_case(processor, "sql", count, 1) != 0) goto cleanup;
    rc = 0;

cleanup:
    if (processor) cmd_processor_shutdown(processor);
    return rc;
}
