#include <stdlib.h>
#include <string.h>

#include "bptree.h"

#define BPTREE_MIN_ORDER 8
#define BPTREE_DEFAULT_ORDER 8
#define BPTREE_MAX_ORDER 256
#define BPTREE_MAX_KEYS (BPTREE_MAX_ORDER - 1)

typedef struct BPlusNode {
    int is_leaf;
    int key_count;
    long keys[BPTREE_MAX_ORDER];
    int values[BPTREE_MAX_ORDER];
    struct BPlusNode *children[BPTREE_MAX_ORDER + 1];
    struct BPlusNode *next;
} BPlusNode;

struct BPlusTree {
    BPlusNode *root;
    int order;
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

static int normalize_order(int order) {
    if (order <= 8) return 8;
    if (order <= 16) return 16;
    if (order <= 32) return 32;
    if (order <= 64) return 64;
    if (order <= 128) return 128;
    return 256;
}

static int choose_order_for_count(int count) {
    if (count < 4096) return 8;
    if (count < 65536) return 16;
    if (count < 262144) return 32;
    return 256;
}

int bptree_recommended_order(int count) {
    return choose_order_for_count(count);
}

static int tree_order(const BPlusTree *tree) {
    return tree ? normalize_order(tree->order) : BPTREE_DEFAULT_ORDER;
}

static int tree_max_keys(const BPlusTree *tree) {
    return tree_order(tree) - 1;
}

static int tree_min_leaf_keys(const BPlusTree *tree) {
    return tree_max_keys(tree) / 2;
}

static int lower_bound_long(const long *keys, int count, long key) {
    int lo = 0;
    int hi = count;

    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (keys[mid] < key) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

static int upper_bound_long(const long *keys, int count, long key) {
    int lo = 0;
    int hi = count;

    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (keys[mid] <= key) lo = mid + 1;
        else hi = mid;
    }
    return lo;
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

static long first_key(BPlusNode *node) {
    while (node && !node->is_leaf) node = node->children[0];
    return (node && node->key_count > 0) ? node->keys[0] : 0;
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
    tree->order = BPTREE_DEFAULT_ORDER;
    tree->root = create_node(1);
    if (!tree->root) {
        free(tree);
        return NULL;
    }
    return tree;
}

BPlusTree *bptree_create_with_order(int order) {
    BPlusTree *tree = bptree_create();
    if (!tree) return NULL;
    tree->order = normalize_order(order);
    return tree;
}

int bptree_order(const BPlusTree *tree) {
    return tree_order(tree);
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

int bptree_build_from_sorted(BPlusTree *tree, const BPlusPair *pairs, int count) {
    BPlusNode **level = NULL;
    BPlusNode **next_level = NULL;
    BPlusNode *new_root = NULL;
    int level_count;
    int next_level_count = 0;
    int order;
    int max_keys;
    int i;

    if (!tree || count < 0) return 0;
    tree->order = choose_order_for_count(count);
    order = tree_order(tree);
    max_keys = tree_max_keys(tree);
    if (count == 0) {
        new_root = create_node(1);
        if (!new_root) return 0;
        destroy_node(tree->root);
        tree->root = new_root;
        return 1;
    }
    if (!pairs) return 0;
    for (i = 1; i < count; i++) {
        if (pairs[i - 1].key >= pairs[i].key) return 0;
    }

    level_count = (count + max_keys - 1) / max_keys;
    level = (BPlusNode **)calloc((size_t)level_count, sizeof(BPlusNode *));
    if (!level) return 0;

    for (i = 0; i < level_count; i++) {
        int start = i * max_keys;
        int leaf_count = count - start;
        int j;

        if (leaf_count > max_keys) leaf_count = max_keys;
        level[i] = create_node(1);
        if (!level[i]) goto fail;
        level[i]->key_count = leaf_count;
        for (j = 0; j < leaf_count; j++) {
            level[i]->keys[j] = pairs[start + j].key;
            level[i]->values[j] = pairs[start + j].row_index;
        }
        if (i > 0) level[i - 1]->next = level[i];
    }

    while (level_count > 1) {
        int parent_count = (level_count + order - 1) / order;
        int parent_idx;

        next_level_count = parent_count;
        next_level = (BPlusNode **)calloc((size_t)parent_count, sizeof(BPlusNode *));
        if (!next_level) goto fail;
        for (parent_idx = 0; parent_idx < parent_count; parent_idx++) {
            int start = parent_idx * order;
            int child_count = level_count - start;
            int child_idx;

            if (child_count > order) child_count = order;
            next_level[parent_idx] = create_node(0);
            if (!next_level[parent_idx]) goto fail;
            next_level[parent_idx]->key_count = child_count - 1;
            for (child_idx = 0; child_idx < child_count; child_idx++) {
                BPlusNode *child = level[start + child_idx];
                next_level[parent_idx]->children[child_idx] = child;
                level[start + child_idx] = NULL;
                if (child_idx > 0) next_level[parent_idx]->keys[child_idx - 1] = first_key(child);
            }
        }
        free(level);
        level = next_level;
        next_level = NULL;
        level_count = parent_count;
    }

    new_root = level[0];
    free(level);
    destroy_node(tree->root);
    tree->root = new_root;
    return 1;

fail:
    if (next_level) {
        for (i = 0; i < next_level_count; i++) destroy_node(next_level[i]);
        free(next_level);
    }
    if (level) {
        for (i = 0; i < level_count; i++) destroy_node(level[i]);
        free(level);
    }
    return 0;
}

int bptree_search(BPlusTree *tree, long key, int *row_index) {
    BPlusNode *node;
    int i;

    if (!tree || !tree->root) return 0;
    node = tree->root;

    while (!node->is_leaf) {
        i = upper_bound_long(node->keys, node->key_count, key);
        node = node->children[i];
    }

    i = lower_bound_long(node->keys, node->key_count, key);
    if (i < node->key_count && node->keys[i] == key) {
        if (row_index) *row_index = node->values[i];
        return 1;
    }
    return 0;
}

int bptree_range_search(BPlusTree *tree, long start_key, long end_key,
                        BPlusRangeVisitor visitor, void *ctx) {
    BPlusNode *node;
    int i;

    if (!tree || !tree->root || !visitor) return 0;
    if (start_key > end_key) return 1;

    node = tree->root;
    while (!node->is_leaf) {
        i = upper_bound_long(node->keys, node->key_count, start_key);
        node = node->children[i];
    }

    while (node) {
        for (i = 0; i < node->key_count; i++) {
            if (node->keys[i] < start_key) continue;
            if (node->keys[i] > end_key) return 1;
            if (!visitor(node->keys[i], node->values[i], ctx)) return 0;
        }
        node = node->next;
    }
    return 1;
}

static int insert_recursive(BPlusTree *tree, BPlusNode *node, long key, int row_index,
                            long *promoted_key, BPlusNode **new_child) {
    int i;
    int max_keys = tree_max_keys(tree);
    int min_leaf_keys = tree_min_leaf_keys(tree);

    if (node->is_leaf) {
        BPlusNode *right = NULL;
        int right_count;

        i = lower_bound_long(node->keys, node->key_count, key);
        if (i < node->key_count && node->keys[i] == key) return 0;
        if (node->key_count == max_keys) {
            right = create_node(1);
            if (!right) return -1;
        }

        memmove(&node->keys[i + 1], &node->keys[i], (size_t)(node->key_count - i) * sizeof(long));
        memmove(&node->values[i + 1], &node->values[i], (size_t)(node->key_count - i) * sizeof(int));
        node->keys[i] = key;
        node->values[i] = row_index;
        node->key_count++;

        if (node->key_count <= max_keys) return 1;

        right_count = node->key_count - min_leaf_keys;
        memcpy(right->keys, &node->keys[min_leaf_keys], (size_t)right_count * sizeof(long));
        memcpy(right->values, &node->values[min_leaf_keys], (size_t)right_count * sizeof(int));
        right->key_count = right_count;
        node->key_count = min_leaf_keys;

        right->next = node->next;
        node->next = right;
        *promoted_key = right->keys[0];
        *new_child = right;
        return 2;
    }

    i = upper_bound_long(node->keys, node->key_count, key);

    long child_key = 0;
    BPlusNode *child = NULL;
    int result = insert_recursive(tree, node->children[i], key, row_index, &child_key, &child);
    if (result != 2) return result;

    BPlusNode *right = NULL;
    int split;
    int right_keys;
    if (node->key_count == max_keys) {
        right = create_node(0);
        if (!right) return -1;
    }

    memmove(&node->keys[i + 1], &node->keys[i], (size_t)(node->key_count - i) * sizeof(long));
    memmove(&node->children[i + 2], &node->children[i + 1], (size_t)(node->key_count - i + 1) * sizeof(BPlusNode *));
    node->keys[i] = child_key;
    node->children[i + 1] = child;
    node->key_count++;

    if (node->key_count <= max_keys) return 1;

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
    int result;

    if (!tree || !tree->root) return -1;

    result = insert_recursive(tree, tree->root, key, row_index, &promoted_key, &new_child);
    if (result != 2) return result;

    new_root = create_node(0);
    if (!new_root) {
        return -1;
    }
    new_root->keys[0] = promoted_key;
    new_root->children[0] = tree->root;
    new_root->children[1] = new_child;
    new_root->key_count = 1;
    tree->root = new_root;
    return 1;
}

int bptree_delete(BPlusTree *tree, long key) {
    BPlusNode *node;
    int i;

    if (!tree || !tree->root) return 0;
    node = tree->root;
    while (!node->is_leaf) {
        i = upper_bound_long(node->keys, node->key_count, key);
        node = node->children[i];
    }

    i = lower_bound_long(node->keys, node->key_count, key);
    if (i < node->key_count && node->keys[i] == key) {
        memmove(&node->keys[i], &node->keys[i + 1],
                (size_t)(node->key_count - i - 1) * sizeof(long));
        memmove(&node->values[i], &node->values[i + 1],
                (size_t)(node->key_count - i - 1) * sizeof(int));
        node->key_count--;
        return 1;
    }
    return 0;
}

typedef struct BPlusStringNode {
    int is_leaf;
    int key_count;
    char *keys[BPTREE_MAX_ORDER];
    int values[BPTREE_MAX_ORDER];
    struct BPlusStringNode *children[BPTREE_MAX_ORDER + 1];
    struct BPlusStringNode *next;
} BPlusStringNode;

struct BPlusStringTree {
    BPlusStringNode *root;
    int order;
};

typedef struct {
    BPlusStringNode **nodes;
    int count;
    int used;
} StringNodePool;

static char *dup_key(const char *src) {
    size_t len;
    char *copy;

    if (!src) return NULL;
    len = strlen(src) + 1;
    copy = (char *)malloc(len);
    if (!copy) return NULL;
    memcpy(copy, src, len);
    return copy;
}

static int string_tree_order(const BPlusStringTree *tree) {
    return tree ? normalize_order(tree->order) : BPTREE_DEFAULT_ORDER;
}

static int string_tree_max_keys(const BPlusStringTree *tree) {
    return string_tree_order(tree) - 1;
}

static int string_tree_min_leaf_keys(const BPlusStringTree *tree) {
    return string_tree_max_keys(tree) / 2;
}

static int lower_bound_string(char *const *keys, int count, const char *key) {
    int lo = 0;
    int hi = count;

    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (strcmp(keys[mid], key) < 0) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

static int upper_bound_string(char *const *keys, int count, const char *key) {
    int lo = 0;
    int hi = count;

    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (strcmp(keys[mid], key) <= 0) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

static BPlusStringNode *create_string_node(int is_leaf) {
    BPlusStringNode *node = (BPlusStringNode *)calloc(1, sizeof(BPlusStringNode));
    if (!node) return NULL;
    node->is_leaf = is_leaf;
    return node;
}

static int string_tree_height(BPlusStringNode *node) {
    int height = 0;
    while (node) {
        height++;
        if (node->is_leaf) break;
        node = node->children[0];
    }
    return height;
}

static char *first_string_key(BPlusStringNode *node) {
    while (node && !node->is_leaf) node = node->children[0];
    return (node && node->key_count > 0) ? node->keys[0] : NULL;
}

static int prepare_string_node_pool(StringNodePool *pool, int count) {
    int i;

    pool->nodes = (BPlusStringNode **)calloc((size_t)count, sizeof(BPlusStringNode *));
    if (!pool->nodes) return 0;
    pool->count = count;
    pool->used = 0;

    for (i = 0; i < count; i++) {
        pool->nodes[i] = create_string_node(1);
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

static BPlusStringNode *take_reserved_string_node(StringNodePool *pool, int is_leaf) {
    BPlusStringNode *node;

    if (!pool || pool->used >= pool->count) return NULL;
    node = pool->nodes[pool->used++];
    memset(node, 0, sizeof(BPlusStringNode));
    node->is_leaf = is_leaf;
    return node;
}

static void release_unused_string_pool_nodes(StringNodePool *pool) {
    int i;

    if (!pool || !pool->nodes) return;
    for (i = pool->used; i < pool->count; i++) free(pool->nodes[i]);
    free(pool->nodes);
    pool->nodes = NULL;
    pool->count = 0;
    pool->used = 0;
}

BPlusStringTree *bptree_string_create(void) {
    BPlusStringTree *tree = (BPlusStringTree *)calloc(1, sizeof(BPlusStringTree));
    if (!tree) return NULL;
    tree->order = BPTREE_DEFAULT_ORDER;
    tree->root = create_string_node(1);
    if (!tree->root) {
        free(tree);
        return NULL;
    }
    return tree;
}

BPlusStringTree *bptree_string_create_with_order(int order) {
    BPlusStringTree *tree = bptree_string_create();
    if (!tree) return NULL;
    tree->order = normalize_order(order);
    return tree;
}

int bptree_string_order(const BPlusStringTree *tree) {
    return string_tree_order(tree);
}

static void destroy_string_node(BPlusStringNode *node) {
    int i;

    if (!node) return;
    for (i = 0; i < node->key_count; i++) free(node->keys[i]);
    if (!node->is_leaf) {
        for (i = 0; i <= node->key_count; i++) {
            destroy_string_node(node->children[i]);
        }
    }
    free(node);
}

void bptree_string_destroy(BPlusStringTree *tree) {
    if (!tree) return;
    destroy_string_node(tree->root);
    free(tree);
}

void bptree_string_clear(BPlusStringTree *tree) {
    if (!tree) return;
    destroy_string_node(tree->root);
    tree->root = create_string_node(1);
}

int bptree_string_build_from_sorted(BPlusStringTree *tree, BPlusStringPair *pairs, int count) {
    BPlusStringNode **level = NULL;
    BPlusStringNode **next_level = NULL;
    BPlusStringNode *new_root = NULL;
    int level_count;
    int next_level_count = 0;
    int order;
    int max_keys;
    int i;

    if (!tree || count < 0) return 0;
    tree->order = choose_order_for_count(count);
    order = string_tree_order(tree);
    max_keys = string_tree_max_keys(tree);
    if (count == 0) {
        new_root = create_string_node(1);
        if (!new_root) return 0;
        destroy_string_node(tree->root);
        tree->root = new_root;
        return 1;
    }
    if (!pairs) return 0;
    for (i = 1; i < count; i++) {
        if (!pairs[i - 1].key || !pairs[i].key || strcmp(pairs[i - 1].key, pairs[i].key) >= 0) return 0;
    }

    level_count = (count + max_keys - 1) / max_keys;
    level = (BPlusStringNode **)calloc((size_t)level_count, sizeof(BPlusStringNode *));
    if (!level) return 0;

    for (i = 0; i < level_count; i++) {
        int start = i * max_keys;
        int leaf_count = count - start;
        int j;

        if (leaf_count > max_keys) leaf_count = max_keys;
        level[i] = create_string_node(1);
        if (!level[i]) goto fail;
        level[i]->key_count = leaf_count;
        for (j = 0; j < leaf_count; j++) {
            level[i]->keys[j] = pairs[start + j].key;
            level[i]->values[j] = pairs[start + j].row_index;
            pairs[start + j].key = NULL;
        }
        if (i > 0) level[i - 1]->next = level[i];
    }

    while (level_count > 1) {
        int parent_count = (level_count + order - 1) / order;
        int parent_idx;

        next_level_count = parent_count;
        next_level = (BPlusStringNode **)calloc((size_t)parent_count, sizeof(BPlusStringNode *));
        if (!next_level) goto fail;
        for (parent_idx = 0; parent_idx < parent_count; parent_idx++) {
            int start = parent_idx * order;
            int child_count = level_count - start;
            int child_idx;

            if (child_count > order) child_count = order;
            next_level[parent_idx] = create_string_node(0);
            if (!next_level[parent_idx]) goto fail;
            next_level[parent_idx]->key_count = child_count - 1;
            for (child_idx = 0; child_idx < child_count; child_idx++) {
                BPlusStringNode *child = level[start + child_idx];
                next_level[parent_idx]->children[child_idx] = child;
                level[start + child_idx] = NULL;
                if (child_idx > 0) {
                    next_level[parent_idx]->keys[child_idx - 1] = dup_key(first_string_key(child));
                    if (!next_level[parent_idx]->keys[child_idx - 1]) goto fail;
                }
            }
        }
        free(level);
        level = next_level;
        next_level = NULL;
        next_level_count = 0;
        level_count = parent_count;
    }

    new_root = level[0];
    free(level);
    destroy_string_node(tree->root);
    tree->root = new_root;
    return 1;

fail:
    if (next_level) {
        for (i = 0; i < next_level_count; i++) destroy_string_node(next_level[i]);
        free(next_level);
    }
    if (level) {
        for (i = 0; i < level_count; i++) destroy_string_node(level[i]);
        free(level);
    }
    return 0;
}

int bptree_string_search(BPlusStringTree *tree, const char *key, int *row_index) {
    BPlusStringNode *node;
    int i;

    if (!tree || !tree->root || !key) return 0;
    node = tree->root;

    while (!node->is_leaf) {
        i = upper_bound_string(node->keys, node->key_count, key);
        node = node->children[i];
    }

    i = lower_bound_string(node->keys, node->key_count, key);
    if (i < node->key_count && strcmp(key, node->keys[i]) == 0) {
        if (row_index) *row_index = node->values[i];
        return 1;
    }
    return 0;
}

int bptree_string_range_search(BPlusStringTree *tree, const char *start_key, const char *end_key,
                               BPlusStringRangeVisitor visitor, void *ctx) {
    BPlusStringNode *node;
    int i;

    if (!tree || !tree->root || !start_key || !end_key || !visitor) return 0;
    if (strcmp(start_key, end_key) > 0) return 1;

    node = tree->root;
    while (!node->is_leaf) {
        i = upper_bound_string(node->keys, node->key_count, start_key);
        node = node->children[i];
    }

    while (node) {
        for (i = 0; i < node->key_count; i++) {
            if (strcmp(node->keys[i], start_key) < 0) continue;
            if (strcmp(node->keys[i], end_key) > 0) return 1;
            if (!visitor(node->keys[i], node->values[i], ctx)) return 0;
        }
        node = node->next;
    }
    return 1;
}

static int string_insert_recursive(BPlusStringTree *tree, BPlusStringNode *node,
                                   const char *key, int row_index,
                                   char **promoted_key, BPlusStringNode **new_child) {
    int i;
    int max_keys = string_tree_max_keys(tree);
    int min_leaf_keys = string_tree_min_leaf_keys(tree);

    if (node->is_leaf) {
        BPlusStringNode *right = NULL;
        char *key_copy;
        char *separator_copy = NULL;
        int right_count;

        i = lower_bound_string(node->keys, node->key_count, key);
        if (i < node->key_count && strcmp(node->keys[i], key) == 0) return 0;
        if (node->key_count == max_keys) {
            right = create_string_node(1);
            if (!right) return -1;
        }
        key_copy = dup_key(key);
        if (!key_copy) return -1;
        if (right) {
            const char *separator;
            if (i < min_leaf_keys) separator = node->keys[min_leaf_keys - 1];
            else if (i == min_leaf_keys) separator = key;
            else separator = node->keys[min_leaf_keys];
            separator_copy = dup_key(separator);
            if (!separator_copy) {
                free(key_copy);
                return -1;
            }
        }

        memmove(&node->keys[i + 1], &node->keys[i], (size_t)(node->key_count - i) * sizeof(char *));
        memmove(&node->values[i + 1], &node->values[i], (size_t)(node->key_count - i) * sizeof(int));
        node->keys[i] = key_copy;
        node->values[i] = row_index;
        node->key_count++;

        if (node->key_count <= max_keys) return 1;

        right_count = node->key_count - min_leaf_keys;
        memcpy(right->keys, &node->keys[min_leaf_keys], (size_t)right_count * sizeof(char *));
        memcpy(right->values, &node->values[min_leaf_keys], (size_t)right_count * sizeof(int));
        memset(&node->keys[min_leaf_keys], 0, (size_t)right_count * sizeof(char *));
        right->key_count = right_count;
        node->key_count = min_leaf_keys;

        right->next = node->next;
        node->next = right;
        *promoted_key = separator_copy;
        *new_child = right;
        return 2;
    }

    i = upper_bound_string(node->keys, node->key_count, key);

    char *child_key = NULL;
    BPlusStringNode *child = NULL;
    int result = string_insert_recursive(tree, node->children[i], key, row_index, &child_key, &child);
    if (result != 2) return result;

    BPlusStringNode *right = NULL;
    int split;
    int right_keys;
    if (node->key_count == max_keys) {
        right = create_string_node(0);
        if (!right) {
            free(child_key);
            return -1;
        }
    }

    memmove(&node->keys[i + 1], &node->keys[i], (size_t)(node->key_count - i) * sizeof(char *));
    memmove(&node->children[i + 2], &node->children[i + 1], (size_t)(node->key_count - i + 1) * sizeof(BPlusStringNode *));
    node->keys[i] = child_key;
    node->children[i + 1] = child;
    node->key_count++;

    if (node->key_count <= max_keys) return 1;

    split = node->key_count / 2;
    *promoted_key = node->keys[split];
    right_keys = node->key_count - split - 1;
    memcpy(right->keys, &node->keys[split + 1], (size_t)right_keys * sizeof(char *));
    memcpy(right->children, &node->children[split + 1], (size_t)(right_keys + 1) * sizeof(BPlusStringNode *));
    memset(&node->keys[split], 0, (size_t)(right_keys + 1) * sizeof(char *));
    memset(&node->children[split + 1], 0, (size_t)(right_keys + 1) * sizeof(BPlusStringNode *));
    right->key_count = right_keys;
    node->key_count = split;
    *new_child = right;
    return 2;
}

int bptree_string_insert(BPlusStringTree *tree, const char *key, int row_index) {
    char *promoted_key = NULL;
    BPlusStringNode *new_child = NULL;
    BPlusStringNode *new_root = NULL;
    int result;

    if (!tree || !tree->root || !key || strlen(key) == 0) return -1;

    result = string_insert_recursive(tree, tree->root, key, row_index, &promoted_key, &new_child);
    if (result != 2) return result;

    new_root = create_string_node(0);
    if (!new_root) {
        free(promoted_key);
        return -1;
    }
    new_root->keys[0] = promoted_key;
    new_root->children[0] = tree->root;
    new_root->children[1] = new_child;
    new_root->key_count = 1;
    tree->root = new_root;
    return 1;
}

int bptree_string_delete(BPlusStringTree *tree, const char *key) {
    BPlusStringNode *node;
    int i;

    if (!tree || !tree->root || !key || strlen(key) == 0) return 0;
    node = tree->root;
    while (!node->is_leaf) {
        i = upper_bound_string(node->keys, node->key_count, key);
        node = node->children[i];
    }

    i = lower_bound_string(node->keys, node->key_count, key);
    if (i < node->key_count && strcmp(key, node->keys[i]) == 0) {
        free(node->keys[i]);
        memmove(&node->keys[i], &node->keys[i + 1],
                (size_t)(node->key_count - i - 1) * sizeof(char *));
        memmove(&node->values[i], &node->values[i + 1],
                (size_t)(node->key_count - i - 1) * sizeof(int));
        node->keys[node->key_count - 1] = NULL;
        node->key_count--;
        return 1;
    }
    return 0;
}
