#include <math.h>
#include <stdio.h>
#include <stdlib.h>

static double nearly_equal(double a, double b) {
    return fabs(a - b) < 1e-9;
}

static double normalize(double value, double ref) {
    if (ref <= 0.0 || value <= 0.0) return 0.0;
    if (value >= ref) return 1.0;
    return value / ref;
}

int main(void) {
    const double ref_util = 0.82;

    double n_id = normalize(1600000.0, 1600000.0);
    double n_uk = (normalize(750000.0, 750000.0) + normalize(750000.0, 750000.0)) / 2.0;
    double n_scan = normalize(160.0, 160.0);
    double n_insert = normalize(140000.0, 140000.0);
    double n_update = normalize(220000.0, 220000.0);
    double n_delete = normalize(120000.0, 120000.0);
    double s_select = 0.60 * n_id + 0.30 * n_uk + 0.10 * n_scan;
    double s_thru = 0.60 * s_select + 0.20 * n_insert + 0.15 * n_update + 0.05 * n_delete;
    double s_util = normalize(ref_util, ref_util);
    double score = 100.0 * (0.60 * s_thru + 0.40 * s_util);

    if (!nearly_equal(score, 100.0)) {
        fprintf(stderr, "[fail] expected score=100.0, got=%.12f\n", score);
        return 1;
    }

    n_delete = 0.70; /* estimated delete policy */
    s_thru = 0.60 * s_select + 0.20 * n_insert + 0.15 * n_update + 0.05 * n_delete;
    score = 100.0 * (0.60 * s_thru + 0.40 * s_util);
    if (!nearly_equal(score, 99.1)) {
        fprintf(stderr, "[fail] expected score=99.1 with weighted score and estimated delete, got=%.12f\n", score);
        return 1;
    }

    n_id = normalize(3200000.0, 1600000.0); /* capped to 1.0 */
    n_uk = (normalize(1500000.0, 750000.0) + normalize(1500000.0, 750000.0)) / 2.0; /* capped to 1.0 */
    n_scan = normalize(320.0, 160.0); /* capped to 1.0 */
    n_insert = normalize(280000.0, 140000.0); /* capped to 1.0 */
    n_update = normalize(440000.0, 220000.0); /* capped to 1.0 */
    n_delete = normalize(240000.0, 120000.0); /* capped to 1.0 */
    s_select = 0.60 * n_id + 0.30 * n_uk + 0.10 * n_scan; /* 1.0 */
    s_thru = 0.60 * s_select + 0.20 * n_insert + 0.15 * n_update + 0.05 * n_delete; /* 1.0 */
    s_util = normalize(1.64, 0.82); /* capped to 1.0 */
    score = 100.0 * (0.60 * s_thru + 0.40 * s_util); /* 100.0 */
    if (!nearly_equal(score, 100.0)) {
        fprintf(stderr, "[fail] expected score=100.0 with cap when perf exceeds reference, got=%.12f\n", score);
        return 1;
    }

    printf("[ok] benchmark formula checks passed\n");
    return 0;
}
