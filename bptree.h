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

BPlusTree *bptree_create(void);
void bptree_destroy(BPlusTree *tree);
int bptree_insert(BPlusTree *tree, long key, int row_index);
int bptree_search(BPlusTree *tree, long key, int *row_index);
void bptree_clear(BPlusTree *tree);
int bptree_build_from_sorted(BPlusTree *tree, const BPlusPair *pairs, int count);

BPlusStringTree *bptree_string_create(void);
void bptree_string_destroy(BPlusStringTree *tree);
int bptree_string_insert(BPlusStringTree *tree, const char *key, int row_index);
int bptree_string_search(BPlusStringTree *tree, const char *key, int *row_index);
void bptree_string_clear(BPlusStringTree *tree);
int bptree_string_build_from_sorted(BPlusStringTree *tree, BPlusStringPair *pairs, int count);

#endif
