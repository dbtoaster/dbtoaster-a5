//////////////////////////////////////////////////////////////////////////
// atomset.h
// 
// Operations for managing sets of constraint atoms.  
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_ATOMSET_H
#define PIP_ATOMSET_H

#define ATOMSET_CACHE(set, type) ((type)set->data)

pip_atomset *pip_atomset_from_clause(int count, pip_atom **clause, int cachesize)
pip_atomset *pip_atomset_by_appending(pip_atomset *oldset, int count, pip_atom **clause)

bool pip_atomset_satisfied_seed(pip_atomset *set, int seed);


#endif
