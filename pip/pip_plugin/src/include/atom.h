//////////////////////////////////////////////////////////////////////////
// atom.h
// 
// Operations for managing constraint atoms.  Atoms are stored in the
// form atom->left > atom->right, where both left and right are flattened
// pip_eqn_component s.  Consequenstly, most of the operations here are
// constructed from their pip_eqn_cmpnt counterparts.
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_ATOM_H
#define PIP_ATOM_H

int pip_atom_sprint(char *str, int len, pip_atom *atom);
int pip_atom_parse(char *str, pip_atom **atom, int *size_ret);
void pip_atom_log(pip_atom *atom);
int pip_extract_clause(HeapTupleHeader row, pip_atom ***out);
pip_atom *pip_atom_compose_cmpnt(pip_eqn_component *left, int left_size, pip_eqn_component *right, int right_size);
pip_atom *pip_atom_compose(pip_eqn *left, pip_eqn *right);
pip_atom *pip_atom_compose_gtf(pip_eqn *left, float8 right);
pip_atom *pip_atom_compose_ltf(float8 left, pip_eqn *right);

int pip_atom_has_var(pip_atom *atom, pip_var *var);
void pip_clause_to_cset(int clause_cnt, pip_atom **clause, pip_cset *set);
int pip_group_atoms(pip_cset *variables, pip_cset_element *group, int clause_cnt, pip_atom **clause, int atoms_complete);

bool pip_atom_evaluate_seed(pip_atom *atom, int64 seed);
bool pip_atom_evaluate_sample(pip_atom *atom, pip_sample_set *set, int sample);

#endif
