#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/typcache.h"
#include "utils/memutils.h"
#include "access/heapam.h"

#include "pip.h"
#include "eqn.h"
#include "atom.h"
#include "atomset.h"

pip_atomset *pip_atomset_create(pip_atomset *oldset, int count, pip_atom **clause, int cachesize);

void pip_atomset_log(pip_atomset *set)
{
  int size = set->cachesize;
  int i;
  for(i = 0; i < set->count; i++){
    pip_atom_log((pip_atom *)&set->data[size]);
    size += VARSIZE(&set->data[size]);
  }
  if(size + sizeof(pip_atomset) > VARSIZE(set)){
    elog(NOTICE, "ERROR, set too big! (%d > %d)", (int)(size + sizeof(pip_atomset)), (int)VARSIZE(set));
  }
}


pip_atomset *pip_atomset_create(pip_atomset *oldset, int count, pip_atom **clause, int cachesize)
{
  pip_atomset *set;
  int size = 
    (oldset == NULL) 
      ? sizeof(pip_atomset) + cachesize
      : VARSIZE(oldset) 
        + ((cachesize > oldset->cachesize) 
          ? cachesize - oldset->cachesize
          : 0);
  int i;
  
  for(i = 0; i < count; i++){
    size += VARSIZE(clause[i]);
  }
  
  set = palloc0(size);
  SET_VARSIZE(set, size);
  
  set->count = 0;
  if(oldset) {
    memcpy(set, oldset, sizeof(pip_atomset));
    SET_VARSIZE(set, size);
    size = ((cachesize > oldset->cachesize) ? cachesize : oldset->cachesize);
    memcpy(&set->data[size], &oldset->data[oldset->cachesize], VARSIZE(oldset) - sizeof(pip_atomset) - oldset->cachesize);
    size += VARSIZE(oldset) - sizeof(pip_atomset) - oldset->cachesize;
    set->cachesize = (cachesize > oldset->cachesize)
        ? cachesize
        : oldset->cachesize;
  } else {
    size = cachesize;
    set->cachesize = cachesize;
  }
  
  set->probability = NAN;
  set->count += count;
  
  for(i = 0; i < count; i++){
    memcpy(&set->data[size], clause[i], VARSIZE(clause[i]));
    size += VARSIZE(clause[i]);
  }
  
//  if(oldset) pip_atomset_log(oldset);
//  pip_atomset_log(set);
  
  return set;
}

pip_atomset *pip_atomset_from_clause(int count, pip_atom **clause, int cachesize)
{
  return pip_atomset_create(NULL, count, clause, cachesize);
}

pip_atomset *pip_atomset_by_appending(pip_atomset *oldset, int count, pip_atom **clause)
{
  return pip_atomset_create(oldset, count, clause, 0);
}

int pip_atomset_to_clause(pip_atomset *set, pip_atom ***clause)
{
  int size = set->cachesize;
  int i;
  if(*clause == NULL){
    *clause = palloc0(sizeof(pip_atom *) * set->count);
  }
  
  for(i = 0; i < set->count; i++){
    (*clause)[i] = (pip_atom *)&set->data[size];
    size += VARSIZE((*clause)[i]);
  }
  
  return set->count;
}

bool pip_atomset_satisfied_seed(pip_atomset *set, int seed)
{
  int i, size = set->cachesize;
  for(i = 0; i < set->count; i++){
    if(!pip_atom_evaluate_seed((pip_atom *)&set->data[size], seed)){
      return false;
    }
    size += VARSIZE(&set->data[size]);
  }
  return true; 
}

bool pip_atomset_unsatisfied_seed(pip_atomset *set, int seed)
{
  int i, size = set->cachesize;
  for(i = 0; i < set->count; i++){
    if(pip_atom_evaluate_seed((pip_atom *)&set->data[size], seed)){
      return false;
    }
    size += VARSIZE(&set->data[size]);
  }
  return true; 
}
