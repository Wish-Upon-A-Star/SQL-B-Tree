#include "engine_cmd_processor_internal.h"
int planner_cache_init(PlannerCache *cache, int capacity) {

    if (!cache || capacity <= 0) return 0;
    memset(cache, 0, sizeof(*cache));
    cache->entries = (PlannerCacheEntry *)calloc((size_t)capacity, sizeof(PlannerCacheEntry));
    if (!cache->entries) return 0;
    cache->capacity = capacity;
    return db_mutex_init(&cache->mutex);
}

void planner_cache_destroy(PlannerCache *cache) {
    if (!cache) return;
    free(cache->entries);
    db_mutex_destroy(&cache->mutex);
    memset(cache, 0, sizeof(*cache));
}

static int planner_cache_build_template(const char *sql, char *buffer, size_t buffer_size) {
    size_t used = 0;
    int in_string = 0;
    int prev_space = 0;
    if (!sql || !buffer || buffer_size == 0) return 0;
    while (*sql && used + 1 < buffer_size) {
        unsigned char ch = (unsigned char)*sql;
        if (ch == '\'') {
            in_string = !in_string;
            if (!prev_space && used + 1 < buffer_size) {
                buffer[used++] = '?';
                prev_space = 0;
            }
            sql++;
            continue;
        }
        if (in_string) {
            sql++;
            continue;
        }
        if (isdigit(ch)) {
            while (isdigit((unsigned char)*sql)) sql++;
            buffer[used++] = '?';
            prev_space = 0;
            continue;
        }
        if (isspace(ch)) {
            if (!prev_space && used > 0) {
                buffer[used++] = ' ';
                prev_space = 1;
            }
            sql++;
            continue;
        }
        buffer[used++] = (char)toupper(ch);
        prev_space = 0;
        sql++;
    }
    if (used > 0 && buffer[used - 1] == ' ') used--;
    buffer[used] = '\0';
    return used > 0;
}

int planner_cache_lookup(PlannerCache *cache, const char *sql, RoutePlan *route_plan, LockPlan *lock_plan) {
    char key[1024];
    int i;
    if (!cache || !sql || !route_plan || !lock_plan) return 0;
    if (!planner_cache_build_template(sql, key, sizeof(key))) return 0;
    db_mutex_lock(&cache->mutex);
    for (i = 0; i < cache->capacity; i++) {
        if (!cache->entries[i].in_use) continue;
        if (strcmp(cache->entries[i].template_sql, key) == 0) {
            *route_plan = cache->entries[i].route_plan;
            *lock_plan = cache->entries[i].lock_plan;
            cache->entries[i].last_used = ++cache->clock;
            db_mutex_unlock(&cache->mutex);
            return 1;
        }
    }
    db_mutex_unlock(&cache->mutex);
    return 0;
}

int planner_cache_store(PlannerCache *cache, const char *sql, const RoutePlan *route_plan, const LockPlan *lock_plan) {
    char key[1024];
    int i;
    int slot = -1;
    unsigned long long oldest = 0;
    if (!cache || !sql || !route_plan || !lock_plan) return 0;
    if (!planner_cache_build_template(sql, key, sizeof(key))) return 0;
    db_mutex_lock(&cache->mutex);
    for (i = 0; i < cache->capacity; i++) {
        if (!cache->entries[i].in_use) {
            slot = i;
            break;
        }
        if (slot == -1 || cache->entries[i].last_used < oldest) {
            oldest = cache->entries[i].last_used;
            slot = i;
        }
    }
    copy_fixed(cache->entries[slot].template_sql, sizeof(cache->entries[slot].template_sql), key);
    cache->entries[slot].route_plan = *route_plan;
    cache->entries[slot].lock_plan = *lock_plan;
    cache->entries[slot].last_used = ++cache->clock;
    cache->entries[slot].in_use = 1;
    db_mutex_unlock(&cache->mutex);
    return 1;
}

static const char *skip_spaces(const char *text) {
    while (text && *text && isspace((unsigned char)*text)) text++;
    return text;
}

static int keyword_eq(const char *text, const char *keyword) {
    size_t i;
    if (!text || !keyword) return 0;
    for (i = 0; keyword[i]; i++) {
        if (!text[i] || toupper((unsigned char)text[i]) != toupper((unsigned char)keyword[i])) return 0;
    }
    return text[i] == '\0' || isspace((unsigned char)text[i]) || text[i] == '(';
}

static int copy_identifier(char *dst, size_t dst_size, const char *start) {
    size_t used = 0;
    if (!dst || dst_size == 0 || !start) return 0;
    while (*start && !isspace((unsigned char)*start) && *start != ';' && *start != '(' && used + 1 < dst_size) {
        dst[used++] = *start++;
    }
    dst[used] = '\0';
    return used > 0;
}

static int build_sql_plan(const char *sql, int shard_count, RoutePlan *route_plan, LockPlan *lock_plan) {
    const char *cursor;
    char table[256];
    const char *trimmed = skip_spaces(sql);
    memset(route_plan, 0, sizeof(*route_plan));
    memset(lock_plan, 0, sizeof(*lock_plan));

    if (keyword_eq(trimmed, "SELECT")) {
        cursor = strstr(trimmed, "FROM");
        if (!cursor) cursor = strstr(trimmed, "from");
        if (!cursor) return 0;
        cursor = skip_spaces(cursor + 4);
        if (!copy_identifier(table, sizeof(table), cursor)) return 0;
        route_plan->request_class = ENGINE_REQUEST_CLASS_READ;
        lock_plan->targets[0].mode = ENGINE_LOCK_READ;
    } else if (keyword_eq(trimmed, "INSERT")) {
        cursor = strstr(trimmed, "INTO");
        if (!cursor) cursor = strstr(trimmed, "into");
        if (!cursor) return 0;
        cursor = skip_spaces(cursor + 4);
        if (!copy_identifier(table, sizeof(table), cursor)) return 0;
        route_plan->request_class = ENGINE_REQUEST_CLASS_WRITE;
        lock_plan->targets[0].mode = ENGINE_LOCK_WRITE;
    } else if (keyword_eq(trimmed, "UPDATE")) {
        cursor = skip_spaces(trimmed + 6);
        if (!copy_identifier(table, sizeof(table), cursor)) return 0;
        route_plan->request_class = ENGINE_REQUEST_CLASS_WRITE;
        lock_plan->targets[0].mode = ENGINE_LOCK_WRITE;
    } else if (keyword_eq(trimmed, "DELETE")) {
        cursor = strstr(trimmed, "FROM");
        if (!cursor) cursor = strstr(trimmed, "from");
        if (!cursor) return 0;
        cursor = skip_spaces(cursor + 4);
        if (!copy_identifier(table, sizeof(table), cursor)) return 0;
        route_plan->request_class = ENGINE_REQUEST_CLASS_WRITE;
        lock_plan->targets[0].mode = ENGINE_LOCK_WRITE;
    } else {
        return 0;
    }

    route_plan->target_table_count = 1;
    copy_fixed(route_plan->target_tables[0], sizeof(route_plan->target_tables[0]), table);
    route_plan->route_key = (uint32_t)hash_text(table);
    route_plan->target_shard = shard_count > 0 ? (int)(route_plan->route_key % (unsigned long)shard_count) : 0;
    lock_plan->lock_count = 1;
    copy_fixed(lock_plan->targets[0].name, sizeof(lock_plan->targets[0].name), table);
    return 1;
}

int build_request_plan(EngineCmdProcessorState *state, CmdRequest *request, RoutePlan *route_plan, LockPlan *lock_plan) {
    if (!state || !request || !route_plan || !lock_plan) return 0;
    memset(route_plan, 0, sizeof(*route_plan));
    memset(lock_plan, 0, sizeof(*lock_plan));
    if (request->type == CMD_REQUEST_PING) {
        route_plan->request_class = ENGINE_REQUEST_CLASS_CONTROL;
        route_plan->route_key = 0;
        route_plan->target_shard = 0;
        return 1;
    }
    if (request->type != CMD_REQUEST_SQL || !request->sql) return 0;
    if (planner_cache_lookup(&state->planner_cache, request->sql, route_plan, lock_plan)) return 1;
    if (!build_sql_plan(request->sql, state->options.shard_count, route_plan, lock_plan)) return 0;
    planner_cache_store(&state->planner_cache, request->sql, route_plan, lock_plan);
    return 1;
}

