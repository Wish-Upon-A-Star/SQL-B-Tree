#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#include <windows.h>
#endif

#include "executor.h"

typedef enum {
    STRESS_MODE_BPLUS,
    STRESS_MODE_JUNGLE
} StressMode;

typedef struct {
    StressMode mode;
    int count;
} StressConfig;

static void configure_console_encoding(void) {
#if defined(_WIN32)
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
#endif
}

static int consume_flag(const char *arg, const char *flag) {
    return arg && strcmp(arg, flag) == 0;
}

static int parse_count_arg(int argc, char *argv[], int index, int fallback) {
    return (index < argc) ? atoi(argv[index]) : fallback;
}

static int parse_stress_config(int argc, char *argv[], StressConfig *config) {
    if (!config) return 0;
    config->mode = STRESS_MODE_BPLUS;
    config->count = 1000000;

    if (argc < 2) return 0;
    if (consume_flag(argv[1], "--benchmark")) {
        config->mode = STRESS_MODE_BPLUS;
        config->count = parse_count_arg(argc, argv, 2, 1000000);
        return 1;
    }
    if (consume_flag(argv[1], "--benchmark-jungle")) {
        config->mode = STRESS_MODE_JUNGLE;
        config->count = parse_count_arg(argc, argv, 2, 1000000);
        return 1;
    }
    return 0;
}

static int run_stress(const StressConfig *config) {
    if (!config) return 1;
    switch (config->mode) {
        case STRESS_MODE_JUNGLE:
            run_jungle_benchmark(config->count);
            return 0;
        case STRESS_MODE_BPLUS:
        default:
            run_bplus_benchmark(config->count);
            return 0;
    }
}

int main(int argc, char *argv[]) {
    StressConfig config;

    configure_console_encoding();
    if (!parse_stress_config(argc, argv, &config)) {
        fprintf(stderr,
                "usage: %s [--benchmark <rows> | --benchmark-jungle <rows>]\n",
                argc > 0 ? argv[0] : "stress_runner");
        return 1;
    }
    return run_stress(&config);
}
