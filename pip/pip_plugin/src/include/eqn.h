//////////////////////////////////////////////////////////////////////////
// eqn.h                                                        
// 
// Operations for managing composite pvars.  All pvars are stored as
// pip_eqns; A pvar is just a one-node pip_eqn.  Because postgres might
// move our values around arbitrarilly, all pip_eqns are stored as 
// flattened trees of pip_eqn_components.  Thus, all pointers are offsets
// from eqn->data and must be dereferenced first.  A special set of 
// functions is provided for recursively manipulating these components
// 
//////////////////////////////////////////////////////////////////////////

#ifndef PIP_EQN_H
#define PIP_EQN_H

typedef struct pip_eqn_component {
  enum pip_eqn_component_type { 
    PIP_EQN_CONST, // val.c
    PIP_EQN_VAR,   // val.var
    PIP_EQN_MULT,  // val.branch.ptr_left * val.branch.ptr_right
    PIP_EQN_ADD,   // val.branch.ptr_left + val.branch.ptr_right
    PIP_EQN_NEGA,  // - val.ptr
    PIP_EQN_SUB    // val.subscript.ptr [ val.subscript.subscript ]
  } type;
  union {
    float8 c;
    pip_var var;  //pip_var is a varsize!
    struct {
      int16 ptr_left, ptr_right;
    } branch;
    struct {
      int16 ptr;
      int64 subscript;
    } subscript;
    int16 ptr;
  } val;
} pip_eqn_component;

typedef struct eqn_cref {
  int16 offset;
  char *base;
  char data[0];
} eqn_cref;

#define COMPONENT(cref) ((pip_eqn_component *)((eqn_cref *)base+offset))
#define BRANCH_LEFT(cref) (COMPONENT(cref)->val.branch.ptr_left)
#define BRANCH_RIGHT(cref) (COMPONENT(cref)->val.branch.ptr_right)

void log_eqn(int loglevel, char *label, char *base);

////////////////////// Equation Operations

/** IO **/
int pip_eqn_sprint(char *str, int len, pip_eqn *atom);
int pip_eqn_parse(char *str, pip_eqn **eqn);
pip_eqn *pip_eqn_for_var(pip_var *pvar);
pip_var *pip_var_for_eqn(pip_eqn *eqn); // returns null if PIP_EQN_VAR isn't the eqn's root.

/** Sampling **/
float8 pip_eqn_evaluate_seed(pip_eqn *eqn, int64 seed);
float8 pip_eqn_evaluate_sample(pip_eqn *eqn, pip_sample_set *set, int sample);

/** Utility Operations */
pip_eqn *pip_eqn_compose_ee(enum pip_eqn_component_type node_type, pip_eqn *eqn_left, pip_eqn *eqn_right);
pip_eqn *pip_eqn_compose_ef(enum pip_eqn_component_type node_type, pip_eqn *eqn_left, float8 eqn_right);
pip_eqn *pip_eqn_compose_one(enum pip_eqn_component_type node_type, pip_eqn *child);
pip_eqn *pip_eqn_compose_sub(enum pip_eqn_component_type node_type, pip_eqn *child, int64 sub);

////////////////////// Equation Component Operations

/** IO **/
int  pip_eqn_cmpnt_sprint(char *str, int len, char *base, int offset);
int  pip_eqn_cmpnt_parse(char *str, char *base, int offset, int *size);

/** Sampling **/
float8 pip_eqn_cmpnt_evaluate_seed(char *base, int offset, int64 seed);
float8 pip_eqn_cmpnt_evaluate_sample(char *base, int offset, pip_sample_set *set, int sample);

/** Utility Operations */
pip_var *pip_eqn_cmpnt_to_cset(char *base, int offset, pip_cset *set);
bool pip_eqn_cmpnt_has_var(char *base, int offset, pip_var *var);
void pip_eqn_cmpnt_update_pointers(char *base, int offset, int shift);
bool pip_eqn_cmpnt_identical_structure(char *leftbase, int leftoffset, char *rightbase, int rightoffset);
bool pip_eqn_cmpnt_simplify_1var(char *base, int offset, float8 *c, float8 *m, pip_var *var);

#endif
