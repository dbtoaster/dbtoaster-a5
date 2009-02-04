#ifndef PIP_LIST_UTILS_H_SHIELD
#define PIP_LIST_UTILS_H_SHIELD

/************  General  ************/
typedef int (pip_sort_comparator)(void *, void *);

/************  Linked Lists  ************/

//Canonical zero-alloc linked list implementation;  The linked list stores pointers in the
//datastructures it is asked to store, so the first 16 (or 8) bytes of any structure kept in
//the linked list should be defined as void* pointers.

typedef struct pip_list_element {
  struct pip_list_element *next, *prev;
} pip_list_element;

typedef struct pip_list {
  pip_list_element *head, *tail;
  unsigned int size;
} pip_list;

typedef int (pip_list_iterator)(pip_list *list, void *item, void *user);

void pip_list_init(pip_list *list);
void pip_list_append(pip_list *list, void *item);
void pip_list_prepend(pip_list *list, void *item);
void pip_list_remove(pip_list *list, void *item);
void *pip_list_head(pip_list *list);
void *pip_list_next(void *item);
void *pip_list_prev(void *item);
unsigned int pip_list_size(pip_list *list);
void *pip_list_entry_append(pip_list *list, void *item);
void *pip_list_entry_prepend(pip_list *list, void *item);
void pip_list_entry_remove(pip_list *list, void *entry);
void *pip_list_entry_get_item(void *entry);

/************  Connected Sets (Union/Find)  ************/

//This datastructure implements a mono-set that supports the canonical Union/Find operations
//The set is implemented as a binary tree, while Union/Find is implemented with union by rank
//and path compression.

typedef struct pip_cset_element {
  struct pip_cset_element *group_next, *group_prev;
  struct pip_cset_element *group_root;
  pip_list group_children;
  unsigned int group_size;
  void *item;
  struct pip_cset_element *sort_left, *sort_right;
} pip_cset_element;

typedef struct pip_cset {
  struct pip_cset_element *root;
  pip_sort_comparator *cmp;
  unsigned int size;
  bool locked;
} pip_cset;

typedef int (pip_cset_iterator)(pip_cset *set, void *item, void *user);

void pip_cset_init(pip_cset *set, pip_sort_comparator *cmp);
void pip_cset_cleanup(pip_cset *set);
bool pip_cset_add(pip_cset *set, void *item);
void *pip_cset_get(pip_cset *set, void *item);
bool pip_cset_test(pip_cset *set, void *item);
void pip_cset_link(pip_cset *set, void *itemA, void *itemB);
bool pip_cset_test_link(pip_cset *set, void *itemA, void *itemB);
int pip_cset_iterate_elements(pip_cset *set, pip_cset_iterator *func, void *user);
int pip_cset_iterate_roots(pip_cset *set, pip_cset_iterator *func, void *user);
int pip_cset_iterate_group(pip_cset *set, pip_cset_element *item, pip_cset_iterator *func, void *user);
unsigned int pip_cset_size(pip_cset *set);
unsigned int pip_cset_group_size(pip_cset *set, pip_cset_element *element);
void pip_cset_lock(pip_cset *set);

/************  Random Presampler  ************/

//The random presampler generates a sorted list of random integers.
//Though the number of integers must be specified at initialization time, both
//the initialization process and every subsequent retreive operation run in constant
//time and space.  This allows us to perform uniform random sampling from a set without
//needing to perform random accesses into the set; because the random numbers are 
//already sorted, we can do sampling via merge join.

#define PIP_PRESAMPLE_BITS 32
typedef struct pip_presample_tree {
  int64 trackback[PIP_PRESAMPLE_BITS];
  int64 path;
  int64 curr_count;
  int64 rand;
  char rand_pos;
} pip_presample_tree; //281 bytes (if that changes, modify catalog/pg_type.h:pip_sample_generator)

void pip_presample_tree_init(pip_presample_tree *ps_tree, int64 size);
void pip_presample_tree_load(pip_presample_tree *ps_tree, int64 trackback[32], int64 path, int64 curr_count);
void pip_presample_tree_save(pip_presample_tree *ps_tree, int64 trackback[32], int64 *path, int64 *curr_count);
int64 pip_presample_tree_curr(pip_presample_tree *ps_tree);
float8 pip_presample_tree_curr_float(pip_presample_tree *ps_tree);
int64 pip_presample_tree_next(pip_presample_tree *ps_tree, bool *done);
float8 pip_presample_tree_next_float(pip_presample_tree *ps_tree, bool *done);

#endif
