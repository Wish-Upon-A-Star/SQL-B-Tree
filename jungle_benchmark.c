#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "types.h"
#include "jungle_benchmark.h"

static int benchmark_path_exists(const char *filename) {
    struct stat st;

    return filename && stat(filename, &st) == 0;
}

static int jungle_clamp_record_count(int record_count, int minimum) {
    if (record_count < minimum) record_count = minimum;
    if (record_count > MAX_RECORDS) {
        printf("[notice] record count capped at MAX_RECORDS=%d.\n", MAX_RECORDS);
        record_count = MAX_RECORDS;
    }
    return record_count;
}

static const char *jungle_track_for_id(int id) {
    static const char *const tracks[] = {
        "sw_ai_lab", "game_lab", "game_tech_lab"
    };
    return tracks[(id - 1) % 3];
}

static const char *jungle_background_for_id(int id) {
    int bucket = (id - 1) % 100;

    if (bucket < 62) return "student";
    if (bucket < 74) return "newgrad";
    if (bucket < 86) return "incumbent";
    if (bucket < 95) return "switcher";
    return "selftaught";
}

static int jungle_pretest_for_id(int id) {
    long long mixed = (long long)id * 73LL + (long long)((id - 1) % 97) * 19LL + 17LL;
    return 35 + (int)(mixed % 66LL);
}

static void build_jungle_history(int id, const char *background, char *buffer, size_t buffer_size) {
    static const char *const majors[] = {
        "cs", "software", "ai", "game", "math", "physics",
        "stats", "design", "ee", "business", "english", "biology"
    };
    static const char *const incumbent_roles[] = {
        "backend", "frontend", "data", "infra", "qa", "game_client", "game_server"
    };
    static const char *const switcher_roles[] = {
        "designer", "teacher", "marketer", "pm", "sales", "mechanical", "accounting"
    };
    static const char *const selftaught_routes[] = {
        "selftaught", "bootcamp", "indie", "academy"
    };
    int major_idx = ((id - 1) / 3) % (int)(sizeof(majors) / sizeof(majors[0]));

    if (strcmp(background, "student") == 0) {
        int grade = ((id - 1) / 7) % 4 + 1;
        snprintf(buffer, buffer_size, "major_%s_grade_%d", majors[major_idx], grade);
        return;
    }

    if (strcmp(background, "newgrad") == 0) {
        snprintf(buffer, buffer_size, "major_%s_graduate", majors[major_idx]);
        return;
    }

    if (strcmp(background, "incumbent") == 0) {
        int role_idx = ((id - 1) / 5) % (int)(sizeof(incumbent_roles) / sizeof(incumbent_roles[0]));
        int years = ((id - 1) / 11) % 6 + 1;
        snprintf(buffer, buffer_size, "%s_%dy", incumbent_roles[role_idx], years);
        return;
    }

    if (strcmp(background, "switcher") == 0) {
        int role_idx = ((id - 1) / 9) % (int)(sizeof(switcher_roles) / sizeof(switcher_roles[0]));
        int years = ((id - 1) / 13) % 8 + 1;
        snprintf(buffer, buffer_size, "%s_%dy", switcher_roles[role_idx], years);
        return;
    }

    {
        int route_idx = ((id - 1) / 17) % (int)(sizeof(selftaught_routes) / sizeof(selftaught_routes[0]));
        int months = ((((id - 1) / 19) % 9) + 1) * 6;
        snprintf(buffer, buffer_size, "%s_%dm", selftaught_routes[route_idx], months);
    }
}

static void build_jungle_github(int id, const char *background, char *buffer, size_t buffer_size) {
    int bucket = (int)(((long long)id * 29LL + 7LL) % 100LL);

    if ((strcmp(background, "student") == 0 && bucket < 18) ||
        (strcmp(background, "newgrad") == 0 && bucket < 8) ||
        (strcmp(background, "switcher") == 0 && bucket < 10) ||
        (strcmp(background, "selftaught") == 0 && bucket < 6)) {
        snprintf(buffer, buffer_size, "none");
        return;
    }

    snprintf(buffer, buffer_size, "gh_%07d", id);
}

static const char *jungle_status_for_id(int id, int pretest) {
    if (id % 113 == 0) return "withdrawn";
    if (pretest >= 98) return "final_pass";
    if (pretest >= 90) return "final_wait";
    if (pretest >= 80) return "interview_wait";
    if (pretest >= 65) return "pretest_pass";
    if (pretest >= 50) return "submitted";
    return "rejected";
}

void build_jungle_email(int id, char *buffer, size_t buffer_size) {
    snprintf(buffer, buffer_size, "jungle%07d@apply.kr", id);
}

void build_jungle_phone(int id, char *buffer, size_t buffer_size) {
    snprintf(buffer, buffer_size, "010-%04d-%04d", id / 10000, id % 10000);
}

void build_jungle_name(int id, char *buffer, size_t buffer_size) {
    static const char *const surnames[] = {
        "김", "이", "박", "최", "정", "강", "조", "윤", "장", "임",
        "한", "오", "서", "신", "권", "황", "안", "송", "전", "홍"
    };
    static const char *const first_syllables[] = {
        "민", "서", "지", "도", "하", "유", "시", "연",
        "현", "준", "서", "주", "예", "우", "선", "태",
        "건", "채", "아", "다", "소", "태", "유", "가",
        "원", "은", "마", "라", "재", "진", "성", "지",
        "호", "규", "나", "수", "보", "은", "가", "현",
        "루", "은", "류", "리", "하", "보", "서", "슬"
    };
    static const char *const second_syllables[] = {
        "준", "서", "연", "하", "리", "민", "원", "아",
        "우", "진", "아", "현", "솔", "린", "윤", "호",
        "지", "은", "경", "보", "빈", "정", "리", "비",
        "린", "나", "은", "수", "빈", "서", "원", "규",
        "채", "나", "솔", "경", "희", "주", "연", "가",
        "온", "율", "연", "림", "로", "윤", "담", "슬"
    };
    int surname_count = (int)(sizeof(surnames) / sizeof(surnames[0]));
    int first_count = (int)(sizeof(first_syllables) / sizeof(first_syllables[0]));
    int second_count = (int)(sizeof(second_syllables) / sizeof(second_syllables[0]));
    int surname_idx = (id - 1) % surname_count;
    int given_combo = ((id - 1) / surname_count) % (first_count * second_count);
    int first_idx = given_combo / second_count;
    int second_idx = given_combo % second_count;

    snprintf(buffer, buffer_size, "%s%s%s",
             surnames[surname_idx], first_syllables[first_idx], second_syllables[second_idx]);
}

void build_jungle_row_data(int id, char *buffer, size_t buffer_size) {
    const char *track = jungle_track_for_id(id);
    const char *background = jungle_background_for_id(id);
    int pretest = jungle_pretest_for_id(id);
    const char *status = jungle_status_for_id(id, pretest);
    char email[64];
    char phone[32];
    char name[64];
    char history[64];
    char github[32];

    build_jungle_email(id, email, sizeof(email));
    build_jungle_phone(id, phone, sizeof(phone));
    build_jungle_name(id, name, sizeof(name));
    build_jungle_history(id, background, history, sizeof(history));
    build_jungle_github(id, background, github, sizeof(github));
    snprintf(buffer, buffer_size, "%s,%s,%s,%s,%s,%s,%d,%s,%s,2026_spring",
             email, phone, name, track, background, history, pretest, github, status);
}

static void build_jungle_csv_record(int id, char *buffer, size_t buffer_size) {
    const char *track = jungle_track_for_id(id);
    const char *background = jungle_background_for_id(id);
    int pretest = jungle_pretest_for_id(id);
    const char *status = jungle_status_for_id(id, pretest);
    char email[64];
    char phone[32];
    char name[64];
    char history[64];
    char github[32];

    build_jungle_email(id, email, sizeof(email));
    build_jungle_phone(id, phone, sizeof(phone));
    build_jungle_name(id, name, sizeof(name));
    build_jungle_history(id, background, history, sizeof(history));
    build_jungle_github(id, background, github, sizeof(github));
    snprintf(buffer, buffer_size, "%d,%s,%s,%s,%s,%s,%s,%d,%s,%s,2026_spring",
             id, email, phone, name, track, background, history, pretest, github, status);
}

int jungle_ensure_artifacts_absent(void) {
    const char *artifacts[] = {
        JUNGLE_BENCHMARK_CSV,
        "jungle_benchmark_users.delta",
        "jungle_benchmark_users.idx"
    };
    int i;

    for (i = 0; i < (int)(sizeof(artifacts) / sizeof(artifacts[0])); i++) {
        if (!benchmark_path_exists(artifacts[i])) continue;
        printf("[safe-stop] jungle benchmark artifact already exists: %s\n", artifacts[i]);
        printf("[notice] No files were deleted. Remove or rename the artifact manually, then rerun --benchmark-jungle.\n");
        return 0;
    }
    return 1;
}

void generate_jungle_dataset(int record_count, const char *filename) {
    FILE *f;
    int i;
    const char *output = (filename && filename[0]) ? filename : JUNGLE_BENCHMARK_CSV;

    record_count = jungle_clamp_record_count(record_count <= 0 ? 1000000 : record_count, 1);
    if (benchmark_path_exists(output)) {
        printf("[safe-stop] dataset file already exists: %s\n", output);
        printf("[notice] No CSV files were overwritten. Choose a new filename or remove it manually.\n");
        return;
    }

    f = fopen(output, "wb");
    if (!f) {
        printf("[error] dataset file could not be created: %s\n", output);
        return;
    }
    if (fputs(JUNGLE_BENCHMARK_HEADER, f) == EOF) {
        fclose(f);
        printf("[error] dataset header could not be written: %s\n", output);
        return;
    }

    for (i = 1; i <= record_count; i++) {
        char row[512];
        build_jungle_csv_record(i, row, sizeof(row));
        if (fputs(row, f) == EOF || fputc('\n', f) == EOF) {
            fclose(f);
            printf("[error] dataset write failed at row %d.\n", i);
            return;
        }
    }

    if (fclose(f) != 0) {
        printf("[error] dataset file close failed: %s\n", output);
        return;
    }

    printf("[ok] jungle applicant dataset generated: %s (%d rows)\n", output, record_count);
}
