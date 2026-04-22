#include "cmd_processor/cmd_processor.h"
#include "cmd_processor/engine_cmd_processor.h"
#include "cmd_processor/tcp_cmd_processor.h"
#include "executor.h"
#include "thirdparty/cjson/cJSON.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define STORY_TABLE "fruit_mochi_orders"
#define STORY_CSV STORY_TABLE ".csv"
#define STORY_DELTA STORY_TABLE ".delta"
#define STORY_IDX STORY_TABLE ".idx"
#define RESPONSE_BYTES 8192
#define QUICK_BURST_THREADS 4
#define POPUP_MENU_COUNT 5
#define STRESS_CLIENTS 4
#define STRESS_WINDOW_PER_CLIENT 16
#define STRESS_REPORT_STEP 1000

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "[FAIL] %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

typedef struct {
    CmdProcessor *processor;
    TCPCmdProcessor *tcp_server;
    int port;
} StoryServer;

typedef struct {
    int port;
    int index;
    int failed;
    pthread_mutex_t *start_mutex;
    pthread_cond_t *start_cond;
    int *ready_count;
    int *start_flag;
} BurstArg;

typedef struct {
    pthread_mutex_t mutex;
    int completed;
    int next_report;
} StressProgress;

typedef struct {
    int port;
    int client_index;
    int start_index;
    int order_count;
    int failed;
    StressProgress *progress;
} StressArg;

static int g_scene_no = 0;

static const char *k_popup_menus[POPUP_MENU_COUNT] = {
    "망고 모찌",
    "블루베리 모찌",
    "딸기 모찌",
    "망고 포멜로 모찌",
    "누텔라 두바이 모찌",
};

static const char *k_popup_menu_board =
    "메뉴판: 망고 모찌, 블루베리 모찌, 딸기 모찌, 망고 포멜로 모찌, 누텔라 두바이 모찌";

static const char *popup_menu_for_index(int index) {
    if (index < 0) index = 0;
    return k_popup_menus[index % POPUP_MENU_COUNT];
}

static void scene(const char *title) {
    g_scene_no++;
    printf("\n[SCENE %d] %s\n", g_scene_no, title);
}

static void pass_line(const char *message) {
    printf("[PASS] %s\n", message);
}

static void info_line(const char *message) {
    printf("[INFO] %s\n", message);
}

static void set_client_timeout(int fd) {
    struct timeval timeout;
#ifdef SO_NOSIGPIPE
    int no_sigpipe = 1;
#endif

    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#ifdef SO_NOSIGPIPE
    (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe));
#endif
}

static int connect_client(int port) {
    int fd;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_client_timeout(fd);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int send_all_client(int fd, const char *data, size_t len) {
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int send_line(int fd, const char *line) {
    if (send_all_client(fd, line, strlen(line)) != 0) return -1;
    return send_all_client(fd, "\n", 1);
}

static int read_line(int fd, char *buffer, size_t buffer_size) {
    size_t len = 0;

    if (!buffer || buffer_size == 0) return -1;
    while (len + 1 < buffer_size) {
        char ch;
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n == 0) return -1;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (ch == '\n') {
            if (len > 0 && buffer[len - 1] == '\r') len--;
            buffer[len] = '\0';
            return (int)len;
        }
        buffer[len++] = ch;
    }
    return -1;
}

static cJSON *read_json_response(int fd) {
    char line[RESPONSE_BYTES];

    if (read_line(fd, line, sizeof(line)) < 0) return NULL;
    return cJSON_Parse(line);
}

static int json_string_equals(cJSON *root, const char *name, const char *expected) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(root, name);
    return cJSON_IsString(item) && strcmp(item->valuestring, expected) == 0;
}

static int json_string_starts_with(cJSON *root, const char *name, const char *prefix) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(root, name);
    size_t prefix_len;

    if (!cJSON_IsString(item) || !prefix) return 0;
    prefix_len = strlen(prefix);
    return strncmp(item->valuestring, prefix, prefix_len) == 0;
}

static int json_bool_equals(cJSON *root, const char *name, int expected) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(root, name);
    if (expected) return cJSON_IsTrue(item);
    return cJSON_IsFalse(item);
}

static int json_number_equals(cJSON *root, const char *name, int expected) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(root, name);
    return cJSON_IsNumber(item) && item->valueint == expected;
}

static int json_body_contains(cJSON *root, const char *needle) {
    cJSON *body = cJSON_GetObjectItemCaseSensitive(root, "body");
    return cJSON_IsString(body) && strstr(body->valuestring, needle) != NULL;
}

static int expect_response(cJSON *root, const char *id, const char *status, int ok) {
    if (!root) return 1;
    CHECK(json_string_equals(root, "id", id));
    CHECK(json_string_equals(root, "status", status));
    CHECK(json_bool_equals(root, "ok", ok));
    return 0;
}

static int send_json_request(int fd,
                             const char *id,
                             const char *op,
                             const char *sql,
                             cJSON **out_response) {
    cJSON *root;
    char *json;
    int rc;

    if (out_response) *out_response = NULL;
    root = cJSON_CreateObject();
    if (!root) return 1;
    cJSON_AddStringToObject(root, "id", id);
    cJSON_AddStringToObject(root, "op", op);
    if (sql) cJSON_AddStringToObject(root, "sql", sql);

    json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!json) return 1;

    rc = send_line(fd, json);
    cJSON_free(json);
    if (rc != 0) return 1;
    if (out_response) *out_response = read_json_response(fd);
    return out_response && *out_response ? 0 : 1;
}

static int send_json_request_async(int fd,
                                   const char *id,
                                   const char *op,
                                   const char *sql) {
    cJSON *root;
    char *json;
    int rc;

    root = cJSON_CreateObject();
    if (!root) return 1;
    cJSON_AddStringToObject(root, "id", id);
    cJSON_AddStringToObject(root, "op", op);
    if (sql) cJSON_AddStringToObject(root, "sql", sql);

    json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!json) return 1;

    rc = send_line(fd, json);
    cJSON_free(json);
    return rc == 0 ? 0 : 1;
}

static int send_sql_expect(int fd,
                           const char *id,
                           const char *sql,
                           const char *status,
                           int ok,
                           const char *body_fragment) {
    cJSON *response = NULL;

    CHECK(send_json_request(fd, id, "sql", sql, &response) == 0);
    CHECK(expect_response(response, id, status, ok) == 0);
    if (body_fragment) CHECK(json_body_contains(response, body_fragment));
    cJSON_Delete(response);
    return 0;
}

static void cleanup_story_files(void) {
    close_all_tables();
    remove(STORY_CSV);
    remove(STORY_DELTA);
    remove(STORY_IDX);
}

static int write_story_table(void) {
    FILE *file;

    cleanup_story_files();
    file = fopen(STORY_CSV, "w");
    CHECK(file != NULL);
    CHECK(fprintf(file, "id(PK),pickup_code(UK),phone(UK),menu(NN),status\n") > 0);
    CHECK(fclose(file) == 0);
    return 0;
}

static int start_story_server(StoryServer *server) {
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;
    TCPCmdProcessorConfig tcp_config;

    memset(server, 0, sizeof(*server));
    memset(&context, 0, sizeof(context));
    context.name = "fruit_mochi_kiosk_story_engine";
    context.max_sql_len = 4095;
    context.request_buffer_count = 96;
    context.response_body_capacity = 4096;

    memset(&options, 0, sizeof(options));
    options.worker_count = 2;
    options.shard_count = 1;
    options.queue_capacity_per_shard = 64;
    options.planner_cache_capacity = 128;

    CHECK(engine_cmd_processor_create(&context, &options, &server->processor) == 0);
    tcp_cmd_processor_config_init(&tcp_config, server->processor);
    CHECK(tcp_cmd_processor_start(&tcp_config, &server->tcp_server) == 0);
    server->port = tcp_cmd_processor_get_port(server->tcp_server);
    CHECK(server->port > 0);
    return 0;
}

static void stop_story_server(StoryServer *server) {
    if (!server) return;
    if (server->tcp_server) {
        tcp_cmd_processor_stop(server->tcp_server);
        server->tcp_server = NULL;
    }
    if (server->processor) {
        cmd_processor_shutdown(server->processor);
        server->processor = NULL;
    }
    close_all_tables();
}

static int scenario_ping(StoryServer *server) {
    int fd;
    cJSON *response = NULL;

    scene("과일모찌 팝업스토어 키오스크 API 서버 오픈");
    info_line(k_popup_menu_board);
    fd = connect_client(server->port);
    CHECK(fd >= 0);
    CHECK(send_json_request(fd, "ping-open", "ping", NULL, &response) == 0);
    CHECK(expect_response(response, "ping-open", "OK", 1) == 0);
    CHECK(json_body_contains(response, "pong"));
    cJSON_Delete(response);
    close(fd);
    pass_line("외부 클라이언트가 TCP API 서버에 접속했고 ping 응답을 받았습니다.");
    return 0;
}

static int scenario_basic_crud(StoryServer *server) {
    int fd;
    cJSON *response = NULL;

    scene("첫 손님이 딸기 모찌를 주문");
    fd = connect_client(server->port);
    CHECK(fd >= 0);
    CHECK(send_sql_expect(fd,
                          "order-1",
                          "INSERT INTO " STORY_TABLE " VALUES (1,'PICKUP-STRAWBERRY-0001','010-0000-0001','딸기 모찌','queued')",
                          "OK",
                          1,
                          "INSERT affected_rows=1") == 0);
    CHECK(send_json_request(fd,
                            "find-1",
                            "sql",
                            "SELECT * FROM " STORY_TABLE " WHERE id = 1",
                            &response) == 0);
    CHECK(expect_response(response, "find-1", "OK", 1) == 0);
    CHECK(json_body_contains(response, "SELECT matched_rows=1"));
    CHECK(json_number_equals(response, "row_count", 1));
    cJSON_Delete(response);

    CHECK(send_sql_expect(fd,
                          "bake-1",
                          "UPDATE " STORY_TABLE " SET status = 'baked' WHERE id = 1",
                          "OK",
                          1,
                          "UPDATE affected_rows=1") == 0);
    CHECK(send_sql_expect(fd,
                          "ship-1",
                          "DELETE FROM " STORY_TABLE " WHERE id = 1",
                          "OK",
                          1,
                          "DELETE affected_rows=1") == 0);
    CHECK(send_sql_expect(fd,
                          "after-pickup-1",
                          "SELECT * FROM " STORY_TABLE " WHERE pickup_code = 'PICKUP-STRAWBERRY-0001'",
                          "OK",
                          1,
                          "SELECT matched_rows=0") == 0);
    close(fd);
    pass_line("INSERT -> SELECT -> UPDATE -> DELETE 전체 흐름이 API 요청으로 통과했습니다.");
    return 0;
}

static int scenario_duplicate_pickup_code(StoryServer *server) {
    int fd;

    scene("품절 직전, 같은 픽업번호를 두 번 받은 손님 등장");
    info_line("망고 포멜로 모찌 주문을 먼저 접수하고, 같은 픽업번호의 누텔라 두바이 모찌 주문을 일부러 다시 보냅니다.");
    fd = connect_client(server->port);
    CHECK(fd >= 0);
    CHECK(send_sql_expect(fd,
                          "limited-first",
                          "INSERT INTO " STORY_TABLE " VALUES (10,'LIMITED-PICKUP-777','010-0000-0010','망고 포멜로 모찌','queued')",
                          "OK",
                          1,
                          "INSERT affected_rows=1") == 0);
    info_line("아래 [error]는 중복 픽업번호 차단을 검증하기 위한 의도된 DB 엔진 로그입니다.");
    CHECK(send_sql_expect(fd,
                          "limited-duplicate",
                          "INSERT INTO " STORY_TABLE " VALUES (11,'LIMITED-PICKUP-777','010-0000-0011','누텔라 두바이 모찌','queued')",
                          "PROCESSING_ERROR",
                          0,
                          NULL) == 0);
    CHECK(send_sql_expect(fd,
                          "limited-survivor",
                          "SELECT * FROM " STORY_TABLE " WHERE pickup_code = 'LIMITED-PICKUP-777'",
                          "OK",
                          1,
                          "SELECT matched_rows=1") == 0);
    info_line("누텔라 두바이 모찌는 새 픽업번호로 재주문해 정상 접수되는지 확인합니다.");
    CHECK(send_sql_expect(fd,
                          "limited-retry-new-code",
                          "INSERT INTO " STORY_TABLE " VALUES (12,'NUTELLA-DUBAI-001','010-0000-0012','누텔라 두바이 모찌','queued')",
                          "OK",
                          1,
                          "INSERT affected_rows=1") == 0);
    close(fd);
    pass_line("B+ Tree UK 인덱스가 중복 픽업번호를 막고, 새 픽업번호 주문은 통과시켰습니다.");
    return 0;
}

static int scenario_invalid_requests(StoryServer *server) {
    int fd;
    cJSON *response = NULL;

    scene("터치패널이 삐끗해서 이상한 JSON과 빈 주문서를 전송");
    fd = connect_client(server->port);
    CHECK(fd >= 0);
    CHECK(send_line(fd, "{bad json") == 0);
    response = read_json_response(fd);
    CHECK(expect_response(response, "unknown", "BAD_REQUEST", 0) == 0);
    cJSON_Delete(response);
    CHECK(send_json_request(fd, "missing-sql", "sql", NULL, &response) == 0);
    CHECK(expect_response(response, "missing-sql", "BAD_REQUEST", 0) == 0);
    cJSON_Delete(response);
    close(fd);
    pass_line("잘못된 API 요청은 DB 엔진까지 내려가기 전에 거절됩니다.");
    return 0;
}

static void *burst_worker(void *arg_ptr) {
    BurstArg *arg = (BurstArg *)arg_ptr;
    int fd;
    char id[64];
    char sql[512];
    char ticket[64];
    char phone[64];
    const char *menu;

    pthread_mutex_lock(arg->start_mutex);
    (*arg->ready_count)++;
    pthread_cond_broadcast(arg->start_cond);
    while (!*arg->start_flag) pthread_cond_wait(arg->start_cond, arg->start_mutex);
    pthread_mutex_unlock(arg->start_mutex);

    fd = connect_client(arg->port);
    if (fd < 0) {
        arg->failed = 1;
        return NULL;
    }

    snprintf(id, sizeof(id), "burst-%02d", arg->index);
    snprintf(ticket, sizeof(ticket), "ORDER-STORM-%02d", arg->index);
    snprintf(phone, sizeof(phone), "010-2026-%04d", arg->index);
    menu = popup_menu_for_index(arg->index);
    snprintf(sql,
             sizeof(sql),
             "INSERT INTO " STORY_TABLE " VALUES (%d,'%s','%s','%s','queued')",
             100 + arg->index,
             ticket,
             phone,
             menu);

    if (send_sql_expect(fd, id, sql, "OK", 1, "INSERT affected_rows=1") != 0) {
        arg->failed = 1;
    }
    close(fd);
    return NULL;
}

static int scenario_concurrent_orders(StoryServer *server) {
    pthread_t threads[QUICK_BURST_THREADS];
    BurstArg args[QUICK_BURST_THREADS];
    pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t start_cond = PTHREAD_COND_INITIALIZER;
    int ready_count = 0;
    int start_flag = 0;
    int i;
    EngineCmdProcessorStats stats;

    scene("팝업 마감 10초 전, 키오스크 여러 대에서 주문 버튼 연타");
    info_line("동시 주문 메뉴: 망고 모찌, 블루베리 모찌, 딸기 모찌, 망고 포멜로 모찌");
    for (i = 0; i < QUICK_BURST_THREADS; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].port = server->port;
        args[i].index = i;
        args[i].start_mutex = &start_mutex;
        args[i].start_cond = &start_cond;
        args[i].ready_count = &ready_count;
        args[i].start_flag = &start_flag;
        CHECK(pthread_create(&threads[i], NULL, burst_worker, &args[i]) == 0);
    }

    pthread_mutex_lock(&start_mutex);
    while (ready_count < QUICK_BURST_THREADS) pthread_cond_wait(&start_cond, &start_mutex);
    start_flag = 1;
    pthread_cond_broadcast(&start_cond);
    pthread_mutex_unlock(&start_mutex);

    for (i = 0; i < QUICK_BURST_THREADS; i++) {
        CHECK(pthread_join(threads[i], NULL) == 0);
        CHECK(args[i].failed == 0);
    }

    memset(&stats, 0, sizeof(stats));
    CHECK(engine_cmd_processor_snapshot_stats(server->processor, &stats) == 0);
    printf("[INFO] total_requests=%llu peak_request_slots=%llu max_queue_depth=%d\n",
           stats.total_requests,
           stats.peak_request_slots_in_use,
           stats.max_queue_depth);
    pass_line("여러 외부 클라이언트 요청이 request id별로 정상 처리됐습니다.");
    return 0;
}

static int parse_positive_int(const char *text, int fallback) {
    long value;
    char *endptr;

    if (!text || !*text) return fallback;
    value = strtol(text, &endptr, 10);
    if (endptr == text || *endptr != '\0' || value <= 0 || value > 1000000L) return fallback;
    return (int)value;
}

static double now_seconds(void) {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static void build_stress_order(int order_index,
                               char *id,
                               size_t id_size,
                               char *sql,
                               size_t sql_size) {
    int order_id = 10000 + order_index;

    snprintf(id, id_size, "stress-%d", order_id);
    snprintf(sql,
             sql_size,
             "INSERT INTO " STORY_TABLE " VALUES (%d,'STRESS-PICKUP-%06d','019-%04d-%04d','%s','queued')",
             order_id,
             order_index,
             (order_index / 10000) % 10000,
             order_index % 10000,
             popup_menu_for_index(order_index));
}

static int expect_stress_response(cJSON *response) {
    CHECK(response != NULL);
    CHECK(json_string_starts_with(response, "id", "stress-"));
    CHECK(json_string_equals(response, "status", "OK"));
    CHECK(json_bool_equals(response, "ok", 1));
    CHECK(json_body_contains(response, "INSERT affected_rows=1"));
    return 0;
}

static void stress_progress_mark_done(StressProgress *progress) {
    pthread_mutex_lock(&progress->mutex);
    progress->completed++;
    while (progress->next_report > 0 && progress->completed >= progress->next_report) {
        printf("[INFO] stress orders completed=%d\n", progress->next_report);
        progress->next_report += STRESS_REPORT_STEP;
    }
    pthread_mutex_unlock(&progress->mutex);
}

static void *stress_worker(void *arg_ptr) {
    StressArg *arg = (StressArg *)arg_ptr;
    int fd;
    int sent = 0;
    int received = 0;

    fd = connect_client(arg->port);
    if (fd < 0) {
        arg->failed = 1;
        return NULL;
    }

    while (received < arg->order_count) {
        while (sent < arg->order_count &&
               sent - received < STRESS_WINDOW_PER_CLIENT) {
            char id[64];
            char sql[512];
            int order_index = arg->start_index + sent;

            build_stress_order(order_index, id, sizeof(id), sql, sizeof(sql));
            if (send_json_request_async(fd, id, "sql", sql) != 0) {
                arg->failed = 1;
                close(fd);
                return NULL;
            }
            sent++;
        }

        {
            cJSON *response = read_json_response(fd);
            if (expect_stress_response(response) != 0) {
                cJSON_Delete(response);
                arg->failed = 1;
                close(fd);
                return NULL;
            }
            cJSON_Delete(response);
        }
        received++;
        stress_progress_mark_done(arg->progress);
    }

    close(fd);
    return NULL;
}

static int scenario_stress_orders(StoryServer *server, int order_count) {
    pthread_t threads[STRESS_CLIENTS];
    StressArg args[STRESS_CLIENTS];
    StressProgress progress;
    EngineCmdProcessorStats stats;
    int active_clients;
    int base_count;
    int remainder;
    int created = 0;
    int i;
    char message[160];
    char select_sql[128];
    double started_at;
    double elapsed;
    int rc = 0;

    snprintf(message,
             sizeof(message),
             "과일모찌 팝업스토어 키오스크에 주문 %d건이 몰리는 부하 시연",
             order_count);
    scene(message);
    printf("[INFO] 부하 주문은 키오스크 %d대가 window=%d로 5개 메뉴를 순환 INSERT합니다.\n",
           STRESS_CLIENTS,
           STRESS_WINDOW_PER_CLIENT);

    memset(&progress, 0, sizeof(progress));
    CHECK(pthread_mutex_init(&progress.mutex, NULL) == 0);
    progress.next_report = order_count >= STRESS_REPORT_STEP ? STRESS_REPORT_STEP : order_count;

    active_clients = order_count < STRESS_CLIENTS ? order_count : STRESS_CLIENTS;
    base_count = order_count / active_clients;
    remainder = order_count % active_clients;
    started_at = now_seconds();

    for (i = 0; i < active_clients; i++) {
        int count = base_count + (i < remainder ? 1 : 0);
        int start_index = i * base_count + (i < remainder ? i : remainder);

        memset(&args[i], 0, sizeof(args[i]));
        args[i].port = server->port;
        args[i].client_index = i;
        args[i].start_index = start_index;
        args[i].order_count = count;
        args[i].progress = &progress;
        if (pthread_create(&threads[i], NULL, stress_worker, &args[i]) != 0) {
            rc = 1;
            break;
        }
        created++;
    }

    for (i = 0; i < created; i++) {
        if (pthread_join(threads[i], NULL) != 0) rc = 1;
        if (args[i].failed) rc = 1;
    }

    elapsed = now_seconds() - started_at;
    pthread_mutex_destroy(&progress.mutex);
    CHECK(rc == 0);
    CHECK(progress.completed == order_count);

    memset(&stats, 0, sizeof(stats));
    CHECK(engine_cmd_processor_snapshot_stats(server->processor, &stats) == 0);
    printf("[INFO] stress stats total_requests=%llu peak_request_slots=%llu max_queue_depth=%d\n",
           stats.total_requests,
           stats.peak_request_slots_in_use,
           stats.max_queue_depth);

    snprintf(select_sql,
             sizeof(select_sql),
             "SELECT * FROM " STORY_TABLE " WHERE id = %d",
             10000 + order_count - 1);
    {
        int fd = connect_client(server->port);
        CHECK(fd >= 0);
        CHECK(send_sql_expect(fd,
                              "stress-last-select",
                              select_sql,
                              "OK",
                              1,
                              "SELECT matched_rows=1") == 0);
        close(fd);
    }
    printf("[INFO] stress elapsed=%.2f sec throughput=%.1f orders/sec\n",
           elapsed,
           elapsed > 0.0 ? (double)order_count / elapsed : 0.0);
    pass_line("대량 주문 후에도 PK 기반 API 조회가 정상 응답했습니다.");
    return 0;
}

int main(int argc, char **argv) {
    StoryServer server;
    int stress = 0;
    int stress_orders = 50000;
    const char *env_orders;
    int i;
    int rc = 1;

    setvbuf(stdout, NULL, _IONBF, 0);
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--stress") == 0) {
            stress = 1;
        } else if (strncmp(argv[i], "--orders=", 9) == 0) {
            stress = 1;
            stress_orders = parse_positive_int(argv[i] + 9, stress_orders);
        }
    }
    env_orders = getenv("MOCHI_STRESS_ORDERS");
    stress_orders = parse_positive_int(env_orders, stress_orders);

    set_executor_quiet(1);
    info_line("runtime table: " STORY_CSV);
    if (write_story_table() != 0) goto done_without_server;
    if (start_story_server(&server) != 0) goto done_without_server;

    printf("[INFO] TCP API server listening on 127.0.0.1:%d\n", server.port);
    if (stress) {
        if (scenario_stress_orders(&server, stress_orders) != 0) goto done_with_server;
        printf("\n[RESULT] PASS - 과일모찌 키오스크 API 서버 병렬 부하 테스트 성공\n");
    } else {
        if (scenario_ping(&server) != 0) goto done_with_server;
        if (scenario_basic_crud(&server) != 0) goto done_with_server;
        if (scenario_duplicate_pickup_code(&server) != 0) goto done_with_server;
        if (scenario_invalid_requests(&server) != 0) goto done_with_server;
        if (scenario_concurrent_orders(&server) != 0) goto done_with_server;
        printf("\n[RESULT] PASS - 과일모찌 키오스크 API 서버 기능 시연 테스트 성공\n");
    }

    rc = 0;

done_with_server:
    stop_story_server(&server);
done_without_server:
    close_all_tables();
    return rc;
}
