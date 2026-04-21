#ifndef BENCH_MEMTRACK_H
#define BENCH_MEMTRACK_H

#if defined(BENCH_MEMTRACK)
typedef struct BenchAllocHeader {
    size_t requested;
} BenchAllocHeader;

static size_t g_bench_live_requested_bytes = 0;
static size_t g_bench_peak_requested_bytes = 0;

static void bench_memtrack_update_peak(void) {
    if (g_bench_live_requested_bytes > g_bench_peak_requested_bytes) {
        g_bench_peak_requested_bytes = g_bench_live_requested_bytes;
    }
}

static void *bench_malloc(size_t size) {
    BenchAllocHeader *header;
    size_t total;

    if (size > ((size_t)-1) - sizeof(BenchAllocHeader)) return NULL;
    total = size + sizeof(BenchAllocHeader);
    header = (BenchAllocHeader *)(malloc)(total);
    if (!header) return NULL;
    header->requested = size;
    g_bench_live_requested_bytes += size;
    bench_memtrack_update_peak();
    return (void *)(header + 1);
}

static void *bench_calloc(size_t nmemb, size_t size) {
    size_t requested;
    void *ptr;

    if (nmemb != 0 && size > ((size_t)-1) / nmemb) return NULL;
    requested = nmemb * size;
    ptr = bench_malloc(requested);
    if (ptr) memset(ptr, 0, requested);
    return ptr;
}

static void *bench_realloc(void *ptr, size_t size) {
    BenchAllocHeader *old_header;
    BenchAllocHeader *new_header;
    size_t old_size;
    size_t total;

    if (!ptr) return bench_malloc(size);
    if (size == 0) {
        (free)(((BenchAllocHeader *)ptr) - 1);
        return NULL;
    }

    old_header = ((BenchAllocHeader *)ptr) - 1;
    old_size = old_header->requested;
    if (size > ((size_t)-1) - sizeof(BenchAllocHeader)) return NULL;
    total = size + sizeof(BenchAllocHeader);
    new_header = (BenchAllocHeader *)(realloc)(old_header, total);
    if (!new_header) return NULL;

    if (g_bench_live_requested_bytes >= old_size) g_bench_live_requested_bytes -= old_size;
    else g_bench_live_requested_bytes = 0;
    g_bench_live_requested_bytes += size;
    new_header->requested = size;
    bench_memtrack_update_peak();
    return (void *)(new_header + 1);
}

static void bench_free(void *ptr) {
    BenchAllocHeader *header;
    if (!ptr) return;
    header = ((BenchAllocHeader *)ptr) - 1;
    if (g_bench_live_requested_bytes >= header->requested) {
        g_bench_live_requested_bytes -= header->requested;
    } else {
        g_bench_live_requested_bytes = 0;
    }
    (free)(header);
}

static void bench_memtrack_report(void) {
    const char *enabled = getenv("BENCH_MEMTRACK_REPORT");
    if (enabled && enabled[0] != '\0' && enabled[0] != '0') {
        printf("[memtrack] peak_heap_requested_bytes=%zu current_live_requested_bytes=%zu\n",
               g_bench_peak_requested_bytes, g_bench_live_requested_bytes);
    }
}

#define malloc(sz) bench_malloc(sz)
#define calloc(n, sz) bench_calloc(n, sz)
#define realloc(ptr, sz) bench_realloc(ptr, sz)
#define free(ptr) bench_free(ptr)
#endif

#endif
