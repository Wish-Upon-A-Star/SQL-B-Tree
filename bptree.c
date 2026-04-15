#include <stdlib.h>
#include <string.h>

#include "bptree.h"

#define BPTREE_ORDER 32
#define BPTREE_MAX_KEYS (BPTREE_ORDER - 1)
#define BPTREE_MIN_LEAF_KEYS ((BPTREE_ORDER - 1) / 2)

typedef struct BPlusNode {
    int is_leaf;
    int key_count;
    long keys[BPTREE_ORDER];
    int values[BPTREE_ORDER];
    struct BPlusNode *children[BPTREE_ORDER + 1];
    struct BPlusNode *next;
} BPlusNode;

struct BPlusTree {
    BPlusNode *root;
};

typedef struct {
    BPlusNode **nodes;
    int count;
    int used;
} NodePool;

static BPlusNode *create_node(int is_leaf) {
    BPlusNode *node = (BPlusNode *)calloc(1, sizeof(BPlusNode));
    if (!node) return NULL;
    node->is_leaf = is_leaf;
    return node;
}

static int tree_height(BPlusNode *node) {
    int height = 0;
    while (node) {
        height++;
        if (node->is_leaf) break;
        node = node->children[0];
    }
    return height;
}

static int prepare_node_pool(NodePool *pool, int count) {
    int i;

    pool->nodes = (BPlusNode **)calloc((size_t)count, sizeof(BPlusNode *));
    if (!pool->nodes) return 0;
    pool->count = count;
    pool->used = 0;

    for (i = 0; i < count; i++) {
        pool->nodes[i] = create_node(1);
        if (!pool->nodes[i]) {
            while (--i >= 0) free(pool->nodes[i]);
            free(pool->nodes);
            pool->nodes = NULL;
            pool->count = 0;
            return 0;
        }
    }
    return 1;
}

static BPlusNode *take_reserved_node(NodePool *pool, int is_leaf) {
    BPlusNode *node;

    if (!pool || pool->used >= pool->count) return NULL;
    node = pool->nodes[pool->used++];
    memset(node, 0, sizeof(BPlusNode));
    node->is_leaf = is_leaf;
    return node;
}

static void release_unused_pool_nodes(NodePool *pool) {
    int i;

    if (!pool || !pool->nodes) return;
    for (i = pool->used; i < pool->count; i++) free(pool->nodes[i]);
    free(pool->nodes);
    pool->nodes = NULL;
    pool->count = 0;
    pool->used = 0;
}

BPlusTree *bptree_create(void) {
    BPlusTree *tree = (BPlusTree *)calloc(1, sizeof(BPlusTree));
    if (!tree) return NULL;
    tree->root = create_node(1);
    if (!tree->root) {
        free(tree);
        return NULL;
    }
    return tree;
}

static void destroy_node(BPlusNode *node) {
    if (!node) return;
    if (!node->is_leaf) {
        int i;
        for (i = 0; i <= node->key_count; i++) {
            destroy_node(node->children[i]);
        }
    }
    free(node);
}

void bptree_destroy(BPlusTree *tree) {
    if (!tree) return;
    destroy_node(tree->root);
    free(tree);
}

void bptree_clear(BPlusTree *tree) {
    if (!tree) return;
    destroy_node(tree->root);
    tree->root = create_node(1);
}

int bptree_search(BPlusTree *tree, long key, int *row_index) {
    BPlusNode *node;
    int i;

    if (!tree || !tree->root) return 0;
    node = tree->root;

    while (!node->is_leaf) {
        i = 0;
        while (i < node->key_count && key >= node->keys[i]) i++;
        node = node->children[i];
    }

    for (i = 0; i < node->key_count; i++) {
        if (node->keys[i] == key) {
            if (row_index) *row_index = node->values[i];
            return 1;
        }
    }
    return 0;
}

static int insert_recursive(BPlusNode *node, long key, int row_index, long *promoted_key, BPlusNode **new_child, NodePool *pool) {
    int i;

    if (node->is_leaf) {
        BPlusNode *right = NULL;
        int right_count;

        i = 0;
        while (i < node->key_count && node->keys[i] < key) i++;
        if (i < node->key_count && node->keys[i] == key) return 0;
        if (node->key_count == BPTREE_MAX_KEYS) {
            right = take_reserved_node(pool, 1);
            if (!right) return -1;
        }

        memmove(&node->keys[i + 1], &node->keys[i], (size_t)(node->key_count - i) * sizeof(long));
        memmove(&node->values[i + 1], &node->values[i], (size_t)(node->key_count - i) * sizeof(int));
        node->keys[i] = key;
        node->values[i] = row_index;
        node->key_count++;

        if (node->key_count <= BPTREE_MAX_KEYS) return 1;

        right_count = node->key_count - BPTREE_MIN_LEAF_KEYS;
        memcpy(right->keys, &node->keys[BPTREE_MIN_LEAF_KEYS], (size_t)right_count * sizeof(long));
        memcpy(right->values, &node->values[BPTREE_MIN_LEAF_KEYS], (size_t)right_count * sizeof(int));
        right->key_count = right_count;
        node->key_count = BPTREE_MIN_LEAF_KEYS;

        right->next = node->next;
        node->next = right;
        *promoted_key = right->keys[0];
        *new_child = right;
        return 2;
    }

    i = 0;
    while (i < node->key_count && key >= node->keys[i]) i++;

    long child_key = 0;
    BPlusNode *child = NULL;
    int result = insert_recursive(node->children[i], key, row_index, &child_key, &child, pool);
    if (result != 2) return result;

    BPlusNode *right = NULL;
    int split;
    int right_keys;
    if (node->key_count == BPTREE_MAX_KEYS) {
        right = take_reserved_node(pool, 0);
        if (!right) return -1;
    }

    memmove(&node->keys[i + 1], &node->keys[i], (size_t)(node->key_count - i) * sizeof(long));
    memmove(&node->children[i + 2], &node->children[i + 1], (size_t)(node->key_count - i + 1) * sizeof(BPlusNode *));
    node->keys[i] = child_key;
    node->children[i + 1] = child;
    node->key_count++;

    if (node->key_count <= BPTREE_MAX_KEYS) return 1;

    split = node->key_count / 2;
    *promoted_key = node->keys[split];
    right_keys = node->key_count - split - 1;
    memcpy(right->keys, &node->keys[split + 1], (size_t)right_keys * sizeof(long));
    memcpy(right->children, &node->children[split + 1], (size_t)(right_keys + 1) * sizeof(BPlusNode *));
    right->key_count = right_keys;
    node->key_count = split;
    *new_child = right;
    return 2;
}

int bptree_insert(BPlusTree *tree, long key, int row_index) {
    long promoted_key = 0;
    BPlusNode *new_child = NULL;
    BPlusNode *new_root = NULL;
    NodePool pool = {0};
    int result;

    if (!tree || !tree->root) return -1;
    if (!prepare_node_pool(&pool, tree_height(tree->root) + 2)) return -1;

    result = insert_recursive(tree->root, key, row_index, &promoted_key, &new_child, &pool);
    if (result != 2) {
        release_unused_pool_nodes(&pool);
        return result;
    }

    new_root = take_reserved_node(&pool, 0);
    if (!new_root) {
        release_unused_pool_nodes(&pool);
        return -1;
    }
    new_root->keys[0] = promoted_key;
    new_root->children[0] = tree->root;
    new_root->children[1] = new_child;
    new_root->key_count = 1;
    tree->root = new_root;
    release_unused_pool_nodes(&pool);
    return 1;
}
