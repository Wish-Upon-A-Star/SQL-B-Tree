#ifndef BPTREE_H
#define BPTREE_H

typedef struct BPlusTree BPlusTree;
typedef struct BPlusStringTree BPlusStringTree;

typedef struct {
    long key;
    int row_index;
} BPlusPair;

typedef struct {
    char *key;
    int row_index;
} BPlusStringPair;

typedef int (*BPlusRangeVisitor)(long key, int row_index, void *ctx);
typedef int (*BPlusStringRangeVisitor)(const char *key, int row_index, void *ctx);

BPlusTree *bptree_create(void);
void bptree_destroy(BPlusTree *tree);
int bptree_insert(BPlusTree *tree, long key, int row_index);
int bptree_search(BPlusTree *tree, long key, int *row_index);
int bptree_range_search(BPlusTree *tree, long start_key, long end_key,
                        BPlusRangeVisitor visitor, void *ctx);
void bptree_clear(BPlusTree *tree);
int bptree_build_from_sorted(BPlusTree *tree, const BPlusPair *pairs, int count);

BPlusStringTree *bptree_string_create(void);
void bptree_string_destroy(BPlusStringTree *tree);
int bptree_string_insert(BPlusStringTree *tree, const char *key, int row_index);
int bptree_string_search(BPlusStringTree *tree, const char *key, int *row_index);
int bptree_string_range_search(BPlusStringTree *tree, const char *start_key, const char *end_key,
                               BPlusStringRangeVisitor visitor, void *ctx);
void bptree_string_clear(BPlusStringTree *tree);
int bptree_string_build_from_sorted(BPlusStringTree *tree, BPlusStringPair *pairs, int count);

#endif
