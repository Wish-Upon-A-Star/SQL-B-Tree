#ifndef BPTREE_H
#define BPTREE_H

typedef struct BPlusTree BPlusTree;

BPlusTree *bptree_create(void);
void bptree_destroy(BPlusTree *tree);
int bptree_insert(BPlusTree *tree, long key, int row_index);
int bptree_search(BPlusTree *tree, long key, int *row_index);
void bptree_clear(BPlusTree *tree);

#endif
