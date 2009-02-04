#include <stdlib.h>
#include <stdio.h>
#include "postgres.h"			/* general Postgres declarations */
#include "fmgr.h"
#include "executor/executor.h"
#include "pip.h"

/************  Linked Lists  ************/

void pip_list_init(pip_list *list)
{
  bzero(list, sizeof(pip_list));
}

void pip_list_append(pip_list *list, void *item)
{
  if(list->head == NULL){
    list->head = item;
  } else {
    list->tail->next = item;
  }
  ((pip_list_element *)item)->next = NULL;
  ((pip_list_element *)item)->prev = list->tail;
  list->tail = item;
  list->size++;
}

void pip_list_prepend(pip_list *list, void *item)
{
  if(list->tail == NULL){
    list->tail = item;
  } else {
    list->head->prev = item;
  }
  ((pip_list_element *)item)->prev = NULL;
  ((pip_list_element *)item)->next = list->head;
  list->head = item;
  list->size++;
}

void pip_list_remove(pip_list *list, void *item)
{
  if(((pip_list_element *)item)->prev == NULL){
    list->head = ((pip_list_element *)item)->next;
  } else {
    ((pip_list_element *)item)->prev->next = ((pip_list_element *)item)->next;
  }
  if(((pip_list_element *)item)->next == NULL){
    list->tail = ((pip_list_element *)item)->prev;
  } else {
    ((pip_list_element *)item)->next->prev = ((pip_list_element *)item)->prev;
  }
}

void *pip_list_head(pip_list *list)
{
  return (void *)list->head;
}

void *pip_list_next(void *item)
{
  return ((pip_list_element *)item)->next;
}
void *pip_list_prev(void *item)
{
  return ((pip_list_element *)item)->prev;
}

unsigned int pip_list_size(pip_list *list)
{
  return list->size;
}

typedef struct pip_list_entry {
  struct pip_list_entry *next, *prev;
  void *item;
} pip_list_entry;

void *pip_list_entry_append(pip_list *list, void *item)
{
  pip_list_entry *entry = palloc0(sizeof(pip_list_entry));
  entry->item = item;
  pip_list_append(list, entry);
  return entry;
}
void *pip_list_entry_prepend(pip_list *list, void *item)
{
  pip_list_entry *entry = palloc0(sizeof(pip_list_entry));
  entry->item = item;
  pip_list_prepend(list, entry);
  return entry;
}
void pip_list_entry_remove(pip_list *list, void *entry)
{
  pip_list_remove(list, entry);
  pfree(entry);
}
void *pip_list_entry_get_item(void *entry)
{
  return ((pip_list_entry *)entry)->item;
}


/************  Connected Sets (Union/Find)  ************/

typedef int (pip_cset_call)(pip_cset *set, pip_cset_element *element, pip_cset_iterator *func, void *user);

static void pip_cset_cleanup_recurse(pip_cset_element *element);
static pip_cset_element **pip_cset_find_node(pip_cset *set, pip_cset_element **root, void *item);
static pip_cset_element *pip_cset_find_group_root(pip_cset_element *element, bool locked);
static int pip_cset_dfs(pip_cset *set, pip_cset_element *curr, pip_cset_call *call, pip_cset_iterator *func, void *user);
static int pip_cset_group_dfs(pip_cset *set, pip_cset_element *curr, pip_cset_iterator *func, void *user);
static int pip_cset_call_item(pip_cset *set, pip_cset_element *element, pip_cset_iterator *func, void *user);
static int pip_cset_call_root(pip_cset *set, pip_cset_element *element, pip_cset_iterator *func, void *user);

void pip_cset_init(pip_cset *set, pip_sort_comparator *cmp)
{
  bzero(set, sizeof(pip_cset));
  set->cmp = cmp;
}

static void pip_cset_cleanup_recurse(pip_cset_element *element){
  if(element == NULL) return;
  pip_cset_cleanup_recurse(element->sort_left);
  pip_cset_cleanup_recurse(element->sort_right);
  pfree(element);
}

void pip_cset_cleanup(pip_cset *set)
{
  pip_cset_cleanup_recurse(set->root);
}

static pip_cset_element **pip_cset_find_node(pip_cset *set, pip_cset_element **root, void *item)
{
  if(*root == NULL){
    //the item's not in the set.  Return a pointer to where it *should* be.
    return root;
  } else {
    int cmp = set->cmp((*root)->item, item);
    if(cmp > 0){
      return pip_cset_find_node(set, &(*root)->sort_right, item);
    } else if(cmp < 0){
      return pip_cset_find_node(set, &(*root)->sort_left, item);
    } else {
      //the two are equal... the item's already in the set.
      return root;
    }
  }
}

bool pip_cset_add(pip_cset *set, void *item)
{
  pip_cset_element **element = pip_cset_find_node(set, &set->root, item);
  if(*element == NULL){ //the item does not exist in the set
    *element = palloc(sizeof(pip_cset_element));
    bzero(*element, sizeof(pip_cset_element));
    (*element)->item = item;
    pip_list_init(&(*element)->group_children);
    (*element)->group_size = 1;
    set->size++;
    return true;
  }
  return false;
}
void *pip_cset_get(pip_cset *set, void *item)
{
  pip_cset_element **element = pip_cset_find_node(set, &set->root, item);
  if(*element == NULL){
    return NULL;
  }
  return (*element)->item;
}
bool pip_cset_test(pip_cset *set, void *item)
{
  pip_cset_element **element = pip_cset_find_node(set, &set->root, item);
  return *element == NULL;
}

static pip_cset_element *pip_cset_find_group_root(pip_cset_element *element, bool locked)
{
  if(element->group_root == NULL){
    return element;
  } else {
    pip_cset_element *root = pip_cset_find_group_root(element->group_root, locked);
    //locking the set fixes the group iteration order
    //this means that element->group_root can not change. The
    //most likely situation in which this would occur is when a group
    //membership query occurs.  In this case we don't fail, but simply
    //skip the path compression step.
    if(!locked && (root != element->group_root)){ 
      pip_list_remove(&element->group_root->group_children, element);
      pip_list_append(&root->group_children, element);
      element->group_root = root;
      //since we're shuffling within a group, we don't need to adjust the root's group size
      //all member group sizes are ignored, so we can ignore them.
    }
    return root;
  }
}

void pip_cset_link(pip_cset *set, void *itemA, void *itemB)
{
  pip_cset_element  **elementA = pip_cset_find_node(set, &set->root, itemA),
                    **elementB = pip_cset_find_node(set, &set->root, itemB),
                     *rootA, *rootB;
  if((*elementA == NULL)||(*elementB == NULL)){
    return;
  }
  if(set->locked){
    return;
  }
  
  rootA = pip_cset_find_group_root(*elementA, set->locked);
  rootB = pip_cset_find_group_root(*elementB, set->locked);
//  elog(NOTICE, "linking %p(%d), %p(%d)", rootA, rootA->group_size, rootB, rootB->group_size);
  if(rootA != rootB){
    if(rootA->group_size < rootB->group_size){
      rootA->group_root = rootB;
      pip_list_append(&rootB->group_children, rootA);
      rootB->group_size += rootA->group_size;
    } else {
      rootB->group_root = rootA;
      pip_list_append(&rootA->group_children, rootB);
//      for(tmp = pip_list_head(&rootA->group_children); tmp != NULL; tmp = pip_list_next(tmp)) elog(NOTICE, "Children now %p->%p", rootA, tmp);
      rootA->group_size += rootB->group_size;
    }
  }
}

bool pip_cset_test_link(pip_cset *set, void *itemA, void *itemB)
{
  pip_cset_element  **elementA = pip_cset_find_node(set, &set->root, itemA),
                    **elementB = pip_cset_find_node(set, &set->root, itemB);
  return pip_cset_find_group_root(*elementA, set->locked) == pip_cset_find_group_root(*elementB, set->locked);
}

static int pip_cset_dfs(pip_cset *set, pip_cset_element *curr, pip_cset_call *call, pip_cset_iterator *func, void *user)
{
  if(curr->sort_left != NULL){
    if(pip_cset_dfs(set, curr->sort_left, call, func, user) < 0) return -1;
  }
  if(call(set, curr, func, user) < 0) return -1;
  if(curr->sort_right != NULL){
    if(pip_cset_dfs(set, curr->sort_right, call, func, user) < 0) return -1;
  }
  return 0;
}

static int pip_cset_call_item(pip_cset *set, pip_cset_element *element, pip_cset_iterator *func, void *user)
{
  return func(set, element->item, user);
}

int pip_cset_iterate_elements(pip_cset *set, pip_cset_iterator *func, void *user)
{
  return pip_cset_dfs(set, set->root, pip_cset_call_item, func, user);
}

static int pip_cset_call_root(pip_cset *set, pip_cset_element *element, pip_cset_iterator *func, void *user) 
{
  if(element->group_root == NULL) return func(set, element, user);
  else return 0;
}

int pip_cset_iterate_roots(pip_cset *set, pip_cset_iterator *func, void *user)
{
  if(set->root == NULL) return -1;
  return pip_cset_dfs(set, set->root, pip_cset_call_root, func, user);
}

static int pip_cset_group_dfs(pip_cset *set, pip_cset_element *curr, pip_cset_iterator *func, void *user)
{
  pip_cset_element *child;
  
//  elog(NOTICE, "Group DFS: %p", curr);
  if(func(set, curr->item, user) < 0) return -1;
  for(child = pip_list_head(&curr->group_children); child != NULL; child = pip_list_next(child)){
    if(pip_cset_group_dfs(set, child, func, user) < 0) return -1;
//    elog(NOTICE, "  Group Child finished: %p->%p -> %p", curr, child, child->group_next);
  }
  return 0;
}

int pip_cset_iterate_group(pip_cset *set, pip_cset_element *element, pip_cset_iterator *func, void *user)
{
  pip_cset_element  *root    = pip_cset_find_group_root(element, set->locked); //almost always a no-op
  
  return pip_cset_group_dfs(set, root, func, user);
}

unsigned int pip_cset_size(pip_cset *set)
{
  return set->size;
}

unsigned int pip_cset_group_size(pip_cset *set, pip_cset_element *element)
{
  pip_cset_element  *root    = pip_cset_find_group_root(element, set->locked); //almost always a no-op
  return root->group_size;
}

void pip_cset_lock(pip_cset *set)
{
  set->locked = 1;
}

/************  Random Presampler  ************/

//The random presampler is built (conceptually) on a trie.  Conceptually, we generate random
//numbers and put them into the trie as they're generated.  The optimization is based on the 
//observation that the bits in a random number can be generated in arbitrary order.
//
//Specifically, if we know that the first bit of a number is one, we can defer generating the
//remaining bits until we've finished outputting all the numbers who's first bit is zero.  Since
//we're batch-deferring, we only need to store the count of the numbers who's first bit is one,
//and we get storage space constant in the total count of numbers being generated.
//
//Apply this same logic recursively for all subsequent bits.


int64 pip_presample_tree_count_bits(pip_presample_tree *ps_tree, int64 size);
bool pip_presample_tree_find_next(pip_presample_tree *ps_tree, int64 bit);

//This little bit of funkiness generates a random number in tree->rand
//and lets the reader access one bit at a time.  Each call to GET_BIT
//returns a unique random bit without the overhead of calling the PRNG
//once for each bit.
//note that random() generates 31 bit random numbers
#define GET_BIT(tree) \
  ((tree)->rand_pos >= ((char)(sizeof(long) * 8)-1)) ?\
    (((tree)->rand = random()) & ((tree)->rand_pos = 1))\
  :\
    (((tree)->rand >> (tree)->rand_pos++) & 0x01)


int64 pip_presample_tree_count_bits(pip_presample_tree *ps_tree, int64 size)
{
  long cnt = 0;
  for(; size > 0; size--){
    cnt += GET_BIT(ps_tree);
  }
  return cnt;
}

void pip_presample_tree_init(pip_presample_tree *ps_tree, int64 size)
{
  int64 i;
//  elog(PRESAMPLE_LOGLEVEL, "presample_tree_init");
  bzero(ps_tree, sizeof(pip_presample_tree));
  ps_tree->rand_pos = (char)sizeof(long) * 8;
  ps_tree->curr_count = size;
  
  //for simplicity's sake we index this array in reverse...
  //this makes it easier to find the right bit in 'path'
  //since we just need to right shift by the same amount as 
  //the index
  for(i = PIP_PRESAMPLE_BITS-1; i >= 0; i--){
    ps_tree->trackback[i] = pip_presample_tree_count_bits(ps_tree, ps_tree->curr_count);
    ps_tree->curr_count -= ps_tree->trackback[i];
    //elog(PIP_PRESAMPLE_LOGLEVEL, "init, generating bit %d (value: %d, remaining: %d)", i, (int)ps_tree->trackback[i], (int)ps_tree->curr_count);
  }
}
//we don't need to save the random number... in fact, we probably shouldn't.  
//It could lead to correlations.  Just make a new one.
void pip_presample_tree_load(pip_presample_tree *ps_tree, int64 trackback[32], int64 path, int64 curr_count){
  bzero(ps_tree, sizeof(pip_presample_tree));
  ps_tree->rand_pos = (char)sizeof(long);
  ps_tree->curr_count = curr_count;
  ps_tree->path = path;
  memcpy(ps_tree->trackback, trackback, sizeof(int64) * 32);
}
void pip_presample_tree_save(pip_presample_tree *ps_tree, int64 trackback[32], int64 *path, int64 *curr_count){
  *path = ps_tree->path;
  *curr_count = ps_tree->curr_count;
  memcpy(trackback, ps_tree->trackback, sizeof(int64) * 32);
}

bool pip_presample_tree_find_next(pip_presample_tree *ps_tree, int64 bit)
{
  int64 i;
  if(bit > 0){
    //elog(PIP_PRESAMPLE_LOGLEVEL, "finding_next, bit %d (value: %d, trackback: %d)", bit, (int)((ps_tree->path >> bit) & 0x01), (int)ps_tree->trackback[bit]);
    if(pip_presample_tree_find_next(ps_tree, bit-1)){
      //If this is true, we've exhausted whatever branch we're on...
      //There's no else clause, since if the function returns false, a child incremented us to the next valid output

      if(((ps_tree->path >> bit) & 0x01) || // if we've exhausted the right branch, we're done.  Go back up the tree.
          (ps_tree->trackback[bit] == 0)){  //similarly, if we know the right branch is empty, we're also done
        return true;
      } else {
        //otherwise... we need to advance the counter at this level.
        //this means we need to reset all subsequent bits, and reinitialize them as in tree_init()
        ps_tree->path = (((ps_tree->path) >> bit) | 0x01) << bit;
        ps_tree->curr_count = ps_tree->trackback[bit];
        for(i = bit-1; i >= 0; i--){
          ps_tree->trackback[i] = pip_presample_tree_count_bits(ps_tree, ps_tree->curr_count);
          ps_tree->curr_count -= ps_tree->trackback[i];
          if(ps_tree->curr_count <= 0){ //if this branch is now empty... might as well skip to the next one.
            ps_tree->path |= (1 << i);
            ps_tree->curr_count = ps_tree->trackback[i];
          }
        }
      }
    }
  } else {
    if(ps_tree->path & 0x01){ // if we've exhausted the right leaf branch, we're done.  Go back up the tree.
      return true;
    } else {
      ps_tree->path |= 0x01;
      ps_tree->curr_count = ps_tree->trackback[bit];
      return ps_tree->curr_count <= 0;
    }
  }
  return false;
}
int64 pip_presample_tree_curr(pip_presample_tree *ps_tree)
{
  return ps_tree->path;
}
float8 pip_presample_tree_curr_float(pip_presample_tree *ps_tree)
{
  return (float8)ps_tree->path / ((float8)(((int64)1)<<PIP_PRESAMPLE_BITS));
}
int64 pip_presample_tree_next(pip_presample_tree *ps_tree, bool *done)
{
  if(ps_tree->curr_count <= 0){
    *done = pip_presample_tree_find_next(ps_tree, PIP_PRESAMPLE_BITS-1);
    if(*done) return 0;
  }
  ps_tree->curr_count --;
  elog(PIP_PRESAMPLE_LOGLEVEL, "sampled: %lld", (long long)ps_tree->path);
  return ps_tree->path;
}
float8 pip_presample_tree_next_float(pip_presample_tree *ps_tree, bool *done)
{
  return ((float8)pip_presample_tree_next(ps_tree, done)) / ((float8)(((int64)1)<<PIP_PRESAMPLE_BITS));
}

