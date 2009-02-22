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

void log_eqn(int loglevel, char *label, char *base)
{
  char output[500];
  output[pip_eqn_cmpnt_sprint(output, 500, base, 0)] = '\0';
  elog(loglevel, "%s:'%s'", label, output);
}

pip_var *pip_eqn_cmpnt_to_cset(char *base, int offset, pip_cset *set)
{
  pip_var *left, *right;
  pip_eqn_component *cmp = DEREF_CMPNT(base,offset);
  switch(cmp->type){
    case PIP_EQN_CONST:
      return NULL;
    case PIP_EQN_VAR:
      pip_cset_add(set, &cmp->val.var);
      return &cmp->val.var;
    case PIP_EQN_MULT:
    case PIP_EQN_ADD:
      left  = pip_eqn_cmpnt_to_cset(base, cmp->val.branch.ptr_left , set);
      right = pip_eqn_cmpnt_to_cset(base, cmp->val.branch.ptr_right, set);
      if(left){
        if(right){
          pip_cset_link(set, left, right);
        }
        return left;
      }
      return right;
    case PIP_EQN_NEGA:
      return pip_eqn_cmpnt_to_cset(base, cmp->val.ptr, set);
    case PIP_EQN_CNSTRT:
      left  = pip_eqn_cmpnt_to_cset(base, cmp->val.branch.ptr_left , set);
      right = pip_eqn_cmpnt_to_cset(DEREF_ATOM(base,cmp->val.branch.ptr_right)->data,
                                    DEREF_ATOM(base,cmp->val.branch.ptr_right)->ptr_right, set);
      if(left){
        if(right){
          pip_cset_link(set, left, right);
        }
        right = left;
      }
      left  = pip_eqn_cmpnt_to_cset(DEREF_ATOM(base,cmp->val.branch.ptr_right)->data,
                                    DEREF_ATOM(base,cmp->val.branch.ptr_right)->ptr_left, set);
      if(left){
        if(right){
          pip_cset_link(set, left, right);
        }
        return left;
      }
      return right;
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", cmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
  return NULL;
}

int pip_eqn_cmpnt_sprint(char *str, int len, char *base, int offset)
{
  int c = 0;
  pip_eqn_component *cmp = DEREF_CMPNT(base,offset);
  switch(cmp->type){
    case PIP_EQN_CONST:
      return snprintf(str, len, "(%lf)", (double)cmp->val.c);
    case PIP_EQN_VAR:
      return pip_var_sprint(str, len, &cmp->val.var);
    case PIP_EQN_MULT:
    case PIP_EQN_ADD:
      str[c] = 
        (cmp->type == PIP_EQN_MULT) ? ( '*' ) : (
        (cmp->type == PIP_EQN_ADD ) ? ( '+' ) : (
        '?' ));
      c++;
      c += pip_eqn_cmpnt_sprint(str+c, len-c, base, cmp->val.branch.ptr_left);
      if(len-c > 0){
        c += pip_eqn_cmpnt_sprint(str+c, len-c, base, cmp->val.branch.ptr_right);
      }
      return c;
    case PIP_EQN_NEGA:
      str[0] = '-';
      return pip_eqn_cmpnt_sprint(str+1, len-1, base, cmp->val.ptr) + 1;
    case PIP_EQN_CNSTRT:
      str[0] = '|';
      c = 1;
      c += pip_eqn_cmpnt_sprint(str+c, len-c, base, cmp->val.branch.ptr_left);
      c += pip_atom_sprint(str+c, len-c, DEREF_ATOM(base, cmp->val.branch.ptr_right));
      return c;
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", cmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
  return 0;
}
int pip_eqn_sprint(char *str, int len, pip_eqn *eqn)
{
  return pip_eqn_cmpnt_sprint(str, len, eqn->data, 0);
}

int pip_eqn_cmpnt_parse(char *str, char *base, int offset, int *size)
{
  char *nextchar;
  int c;
  double val;
  pip_var *pvar = NULL;
  switch(str[0]){
    case '('://constant
      if(sscanf(str, "(%lf)%n", &val, &c) < 1){ return -1; }
      if(size){ *size += sizeof(pip_eqn_component); }
      if(base){ 
        DEREF_CMPNT(base,offset)->type = PIP_EQN_CONST;
        DEREF_CMPNT(base,offset)->val.c = val;
      }
      return c;
      
    case '['://var
      c = pip_var_parse(str, &pvar);
      if(pvar == NULL){ return -2; }
      if(size){ *size += EQN_CMPNT_VAR_SIZE(pvar); }
      if(base){ EQN_CMPNT_SET_VAR(base, offset, pvar); }
      return c;
      
    case '*'://mult
    case '+'://add
      if(base){ DEREF_CMPNT(base,offset)->type = (str[0] == '+') ? PIP_EQN_ADD : PIP_EQN_MULT; }
      nextchar = str+1;
      c = sizeof(pip_eqn_component);
      if(base){ DEREF_CMPNT(base,offset)->val.branch.ptr_left = offset+c; }
      nextchar += pip_eqn_cmpnt_parse(nextchar, base, offset+c, &c);
      if(base){ DEREF_CMPNT(base,offset)->val.branch.ptr_right = offset+c; }
      nextchar += pip_eqn_cmpnt_parse(nextchar, base, offset+c, &c);
      if(size) { (*size) += c; }
      return nextchar - str;
    
    case '-'://negation
      if(base){ 
        DEREF_CMPNT(base,offset)->type = PIP_EQN_NEGA; 
        DEREF_CMPNT(base,offset)->val.ptr = offset+sizeof(pip_eqn_component); 
      }
      if(size){ size += sizeof(pip_eqn_component); };
      return pip_eqn_cmpnt_parse(str+1, base, offset+sizeof(pip_eqn_component), size) + 1;
      
    case '|'://constraint
      {
        int atom_size = 0;
        pip_atom *atom;
        c = sizeof(pip_eqn_component);
        nextchar = str+1;
        if(base){ DEREF_CMPNT(base,offset)->type = PIP_EQN_CNSTRT; }
        if(base){ DEREF_CMPNT(base,offset)->val.branch.ptr_left = offset+c; }
        nextchar += pip_eqn_cmpnt_parse(nextchar, base, offset+c, &c);
        if(base){ DEREF_CMPNT(base,offset)->val.branch.ptr_right = offset+c; }
        atom = DEREF_ATOM(base,offset+c);
        nextchar += pip_atom_parse(nextchar, &atom, &atom_size);
        if(size) { (*size) += c+atom_size; }
        return nextchar - str;
      }
  }
  return 0;
}
int pip_eqn_parse(char *str, pip_eqn **eqn)
{
  int size; 
  pip_eqn_cmpnt_parse(str, NULL, 0, &size);
  *eqn = SPI_palloc(size);
  bzero(*eqn, size);
  SET_VARSIZE(eqn, size);
  return pip_eqn_cmpnt_parse(str, (*eqn)->data, 0, NULL);
}

pip_eqn *pip_eqn_for_var(pip_var *pvar)
{
  int                 eqn_size = EQN_CMPNT_VAR_SIZE(pvar) + sizeof(pip_eqn);
  pip_eqn            *eqn = SPI_palloc(eqn_size);
  
  bzero(eqn, eqn_size);
  SET_VARSIZE(eqn, eqn_size);
  EQN_CMPNT_SET_VAR(eqn->data, 0, pvar);
  
  //log_eqn(NOTICE, "eqn_for_var", eqn->data);

  return eqn;
}
pip_var *pip_var_for_eqn(pip_eqn *eqn)
{
  pip_var            *var;

  if(((pip_eqn_component *)eqn->data)->type != PIP_EQN_VAR) return NULL;

  var = SPI_palloc(VARSIZE(&((pip_eqn_component *)eqn->data)->val.var));
  memcpy(var, &((pip_eqn_component *)eqn->data)->val.var, VARSIZE(&((pip_eqn_component *)eqn->data)->val.var));
  
  return var;
}

float8 pip_eqn_cmpnt_evaluate_seed(char *base, int offset, int64 seed)
{
  float8 val;
  pip_eqn_component *cmp = DEREF_CMPNT(base,offset);
  switch(cmp->type){
    case PIP_EQN_CONST:
      return cmp->val.c;
    case PIP_EQN_VAR:
      return pip_var_gen_w_name_and_seed(&cmp->val.var, seed);
    case PIP_EQN_MULT:
      val =  pip_eqn_cmpnt_evaluate_seed(base, cmp->val.branch.ptr_left , seed);
      val *= pip_eqn_cmpnt_evaluate_seed(base, cmp->val.branch.ptr_right, seed);
      return val;
    case PIP_EQN_ADD:
      val =  pip_eqn_cmpnt_evaluate_seed(base, cmp->val.branch.ptr_left , seed);
      val += pip_eqn_cmpnt_evaluate_seed(base, cmp->val.branch.ptr_right, seed);
      return val;
    case PIP_EQN_NEGA:
      return 0.0 - pip_eqn_cmpnt_evaluate_seed(base, cmp->val.ptr, seed);
    case PIP_EQN_CNSTRT:
      return 
        pip_atom_evaluate_seed(DEREF_ATOM(base, cmp->val.branch.ptr_right), seed) ? 
          pip_eqn_cmpnt_evaluate_seed(base, cmp->val.branch.ptr_left, seed) :
          0.0;
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", cmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
  return 0.0;
}
float8 pip_eqn_evaluate_seed(pip_eqn *eqn, int64 seed)
{
  return pip_eqn_cmpnt_evaluate_seed(eqn->data, 0, seed);
}
float8 pip_eqn_cmpnt_evaluate_sample(char *base, int offset, pip_sample_set *set, int sample)
{
  float8 val;
  pip_eqn_component *cmp = DEREF_CMPNT(base,offset);
  switch(cmp->type){
    case PIP_EQN_CONST:
      return cmp->val.c;
    case PIP_EQN_VAR:
      return pip_sample_var_val(set, sample, &cmp->val.var);
    case PIP_EQN_MULT:
      val =  pip_eqn_cmpnt_evaluate_sample(base, cmp->val.branch.ptr_left , set, sample);
      val *= pip_eqn_cmpnt_evaluate_sample(base, cmp->val.branch.ptr_right, set, sample);
      return val;
    case PIP_EQN_ADD:
      val =  pip_eqn_cmpnt_evaluate_sample(base, cmp->val.branch.ptr_left , set, sample);
      val += pip_eqn_cmpnt_evaluate_sample(base, cmp->val.branch.ptr_right, set, sample);
      return val;
    case PIP_EQN_NEGA:
      return 0.0 - pip_eqn_cmpnt_evaluate_sample(base, cmp->val.ptr, set, sample);
    case PIP_EQN_CNSTRT:
      return 
        pip_atom_evaluate_sample(DEREF_ATOM(base, cmp->val.branch.ptr_right), set, sample) ? 
          pip_eqn_cmpnt_evaluate_sample(base, cmp->val.branch.ptr_left, set, sample) :
          0.0;
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", cmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
  return 0.0;
}
float8 pip_eqn_evaluate_sample(pip_eqn *eqn, pip_sample_set *set, int sample)
{
  return pip_eqn_cmpnt_evaluate_sample(eqn->data, 0, set, sample);
}
bool pip_eqn_cmpnt_has_var(char *base, int offset, pip_var *var)
{
  pip_eqn_component *cmp = DEREF_CMPNT(base,offset);
  switch(cmp->type){
    case PIP_EQN_CONST:
      return false;
    case PIP_EQN_VAR:
      return pip_var_eq(var, &cmp->val.var);
    case PIP_EQN_MULT:
    case PIP_EQN_ADD:
      return 
        pip_eqn_cmpnt_has_var(base, cmp->val.branch.ptr_left , var) ||
        pip_eqn_cmpnt_has_var(base, cmp->val.branch.ptr_right, var);
    case PIP_EQN_NEGA:
      return pip_eqn_cmpnt_has_var(base, cmp->val.ptr, var);
    case PIP_EQN_CNSTRT:
      return
        pip_eqn_cmpnt_has_var(base, cmp->val.branch.ptr_left , var) ||
        pip_atom_has_var(DEREF_ATOM(base, cmp->val.branch.ptr_right), var);
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", cmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
  return false;
}

void pip_eqn_cmpnt_update_pointers(char *base, int offset, int shift)
{
  pip_eqn_component *cmp = DEREF_CMPNT(base,offset);
  switch(cmp->type){
    case PIP_EQN_CONST:
    case PIP_EQN_VAR:
      return;
    case PIP_EQN_MULT:
    case PIP_EQN_ADD:
      pip_eqn_cmpnt_update_pointers(base, cmp->val.branch.ptr_left , shift);
      pip_eqn_cmpnt_update_pointers(base, cmp->val.branch.ptr_right, shift);
      cmp->val.branch.ptr_left  += shift;
      cmp->val.branch.ptr_right += shift;
      return;
    case PIP_EQN_NEGA:
      pip_eqn_cmpnt_update_pointers(base, cmp->val.ptr, shift);
      cmp->val.ptr += shift;
      return;
    case PIP_EQN_CNSTRT:
      pip_eqn_cmpnt_update_pointers(base, cmp->val.branch.ptr_left , shift);
      //the atom's pointers are all self-contained.  no need to update them.
      cmp->val.branch.ptr_left  += shift;
      cmp->val.branch.ptr_right += shift;
      return;
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", cmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
}

pip_eqn *pip_eqn_compose_ee(enum pip_eqn_component_type node_type, pip_eqn *eqn_left, pip_eqn *eqn_right)
{
  pip_eqn *eqn;
  int eqn_size = (VARSIZE(eqn_left) + VARSIZE(eqn_right) - sizeof(pip_eqn) + sizeof(pip_eqn_component)), strpos = 0;
  pip_eqn_component *node;
  eqn = SPI_palloc(eqn_size);
  bzero(eqn, eqn_size);
  SET_VARSIZE(eqn, eqn_size);
  
  node = DEREF_CMPNT(eqn->data, strpos);
  node->type = node_type;
  strpos += sizeof(pip_eqn_component);
  
  memcpy(eqn->data+strpos, eqn_left->data, VARSIZE(eqn_left)-sizeof(pip_eqn));
  pip_eqn_cmpnt_update_pointers(eqn->data+strpos, 0, strpos);
  node->val.branch.ptr_left = strpos;
  strpos += VARSIZE(eqn_left)-sizeof(pip_eqn);
  
  memcpy(eqn->data+strpos, eqn_right->data, VARSIZE(eqn_right)-sizeof(pip_eqn));
  pip_eqn_cmpnt_update_pointers(eqn->data+strpos, 0, strpos);
  node->val.branch.ptr_right = strpos;
  strpos += VARSIZE(eqn_right)-sizeof(pip_eqn);
  
  if(strpos > eqn_size - sizeof(pip_eqn)){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("Overflow creating node eqn(%d): %ld used, %ld available",
                    node_type, (long)(strpos), (long)(eqn_size - sizeof(pip_eqn)))));
    return NULL;
  }
  return eqn;
}

pip_eqn *pip_eqn_compose_ef(enum pip_eqn_component_type node_type, pip_eqn *eqn_left, float8 eqn_right)
{
  pip_eqn_component *node;
  int eqn_size = (VARSIZE(eqn_left) + 2*sizeof(pip_eqn_component)), strpos = 0;
  pip_eqn *eqn;
  
  eqn = SPI_palloc(eqn_size);
  bzero(eqn, eqn_size);
  SET_VARSIZE(eqn, eqn_size);
  
  node = DEREF_CMPNT(eqn->data, strpos);
  node->type = node_type;
  strpos += sizeof(pip_eqn_component);
  
  DEREF_CMPNT(eqn->data, strpos)->val.c = eqn_right;
  DEREF_CMPNT(eqn->data, strpos)->type = PIP_EQN_CONST;
  node->val.branch.ptr_left = strpos;
  strpos += sizeof(pip_eqn_component);
  
  memcpy(eqn->data+strpos, eqn_left->data, VARSIZE(eqn_left)-sizeof(pip_eqn));
  pip_eqn_cmpnt_update_pointers(eqn->data+strpos, 0, strpos);
  node->val.branch.ptr_right = strpos;
  strpos += VARSIZE(eqn_left)-sizeof(pip_eqn);
  
  if(strpos > eqn_size - sizeof(pip_eqn)){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("Overflow creating node eqn(%d): %ld used, %ld available",
                    node_type, (long)(strpos), (long)(eqn_size - sizeof(pip_eqn)))));
    return NULL;
  }
  
  return eqn;
}

pip_eqn *pip_eqn_compose_one(enum pip_eqn_component_type node_type, pip_eqn *child)
{
  pip_eqn_component *node;
  int eqn_size = (VARSIZE(child) + 2*sizeof(pip_eqn_component)), strpos = 0;
  pip_eqn *eqn;
  
  eqn = SPI_palloc(eqn_size);
  bzero(eqn, eqn_size);
  SET_VARSIZE(eqn, eqn_size);
  
  node = DEREF_CMPNT(eqn->data, strpos);
  node->type = node_type;
  strpos += sizeof(pip_eqn_component);
  
  memcpy(eqn->data+strpos, child->data, VARSIZE(child)-sizeof(pip_eqn));
  pip_eqn_cmpnt_update_pointers(eqn->data+strpos, 0, strpos);
  node->val.ptr = strpos;
  strpos += VARSIZE(child)-sizeof(pip_eqn);
  
  if(strpos > eqn_size - sizeof(pip_eqn)){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("Overflow creating node eqn(%d): %ld used, %ld available",
                    node_type, (long)(strpos), (long)(eqn_size - sizeof(pip_eqn)))));
    return NULL;
  }
  
  return eqn;
}

pip_eqn *pip_eqn_compose_sub(enum pip_eqn_component_type node_type, pip_eqn *child, int64 sub)
{
  pip_eqn_component *node;
  int eqn_size = (VARSIZE(child) + 2*sizeof(pip_eqn_component)), strpos = 0;
  pip_eqn *eqn;
  
  eqn = SPI_palloc(eqn_size);
  bzero(eqn, eqn_size);
  SET_VARSIZE(eqn, eqn_size);
  
  node = DEREF_CMPNT(eqn->data, strpos);
  node->type = node_type;
  strpos += sizeof(pip_eqn_component);
  
  memcpy(eqn->data+strpos, child->data, VARSIZE(child)-sizeof(pip_eqn));
  pip_eqn_cmpnt_update_pointers(eqn->data+strpos, 0, strpos);
  node->val.subscript.ptr = strpos;
  node->val.subscript.subscript = sub;
  strpos += VARSIZE(child)-sizeof(pip_eqn);
  
  if(strpos > eqn_size - sizeof(pip_eqn)){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("Overflow creating node eqn(%d): %ld used, %ld available",
                    node_type, (long)(strpos), (long)(eqn_size - sizeof(pip_eqn)))));
    return NULL;
  }
  
  return eqn;
}
pip_eqn *pip_eqn_append_slot_ee(enum pip_eqn_component_type node_type, pip_eqn *source, int size, int *offset)
{
  pip_eqn_component *topnode, *bottomnode;
  int eqn_size = (VARSIZE(source) + sizeof(pip_eqn_component) + size), strpos = 0;
  pip_eqn *eqn;
  
  eqn = SPI_palloc(eqn_size);
  bzero(eqn, eqn_size);
  
  if(DEREF_CMPNT(source->data, 0)->type == node_type){
    //if there's already a node of this type at the head...
    //append_slot assumes that nodes of this type are transitive.
    //consequently, we don't need to rewrite the entire tree.
    memcpy(eqn, source, VARSIZE(source)-sizeof(source->vl_len_));
    SET_VARSIZE(eqn, eqn_size);
    strpos = VARSIZE(source) - sizeof(pip_eqn);
    topnode = DEREF_CMPNT(eqn->data, 0);
    bottomnode = DEREF_CMPNT(eqn->data, strpos);
    bottomnode->type = node_type;
    bottomnode->val.branch.ptr_left = topnode->val.branch.ptr_right;
    topnode->val.branch.ptr_right = strpos;
    strpos += sizeof(pip_eqn_component);
    bottomnode->val.branch.ptr_right = strpos;
    *offset = strpos;
  } else {
    SET_VARSIZE(eqn, eqn_size);
    strpos = sizeof(pip_eqn_component);
    topnode = DEREF_CMPNT(eqn->data, 0);
    topnode->type = node_type;
    topnode->val.branch.ptr_left = strpos;
    memcpy(eqn->data+strpos, source->data, VARSIZE(source)-sizeof(pip_eqn));
    pip_eqn_cmpnt_update_pointers(eqn->data+strpos, 0, strpos);
    strpos += VARSIZE(source)-sizeof(pip_eqn);
    topnode->val.branch.ptr_right = strpos;
    *offset = strpos;
  }
  return eqn;
}

int pip_eqn_fill_in_constraints(char *base, int offset, pip_atom **atoms, int atom_count)
{
  int i;
  
  for(i = 0; i < atom_count; i++){
    DEREF_CMPNT(base, offset)->type = PIP_EQN_CNSTRT;
    DEREF_CMPNT(base, offset)->val.branch.ptr_right = offset+sizeof(pip_eqn_component);
    DEREF_CMPNT(base, offset)->val.branch.ptr_left  = offset+sizeof(pip_eqn_component)+VARSIZE(atoms[i]);
    memcpy(DEREF_ATOM(base, offset+sizeof(pip_eqn_component)), atoms[i], VARSIZE(atoms[i]));
    offset += sizeof(pip_eqn_component)+VARSIZE(atoms[i]);
  }
  
  return offset;
}

bool pip_eqn_cmpnt_identical_structure(char *leftbase, int leftoffset, char *rightbase, int rightoffset)
{
  pip_eqn_component *lcmp = DEREF_CMPNT(leftbase,leftoffset);
  pip_eqn_component *rcmp = DEREF_CMPNT(rightbase,rightoffset);

  if(lcmp->type != rcmp->type) return false;
  switch(lcmp->type){
    case PIP_EQN_CONST:
      return lcmp->val.c == rcmp->val.c;
    case PIP_EQN_VAR:
      return pip_var_eq(&lcmp->val.var, &rcmp->val.var);
    case PIP_EQN_MULT:
    case PIP_EQN_ADD:
      return
        pip_eqn_cmpnt_identical_structure(leftbase, lcmp->val.branch.ptr_left,  rightbase, rcmp->val.branch.ptr_left ) &&
        pip_eqn_cmpnt_identical_structure(leftbase, lcmp->val.branch.ptr_right, rightbase, rcmp->val.branch.ptr_right);
    case PIP_EQN_NEGA:
      return 
        pip_eqn_cmpnt_identical_structure(leftbase, lcmp->val.ptr,  rightbase, rcmp->val.ptr );
    case PIP_EQN_CNSTRT:
      //unhandled case
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", lcmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
  return false;
}

bool pip_eqn_cmpnt_simplify_1var(char *base, int offset, float8 *c, float8 *m, pip_var *var)
{
  pip_eqn_component *cmp = DEREF_CMPNT(base, offset);
  switch(cmp->type){
    case PIP_EQN_CONST:
      *c += cmp->val.c;
      return true;
    case PIP_EQN_VAR:
      if(pip_var_eq(&cmp->val.var, var)){
        *m += 1;
        return true;
      }
      return false;
    case PIP_EQN_MULT:
      {
        float8 lm = 0.0, rm = 0.0, lc = 0.0, rc = 0.0;
        if( pip_eqn_cmpnt_simplify_1var(base, cmp->val.branch.ptr_left,  &lc, &lm, var) &&
            pip_eqn_cmpnt_simplify_1var(base, cmp->val.branch.ptr_right, &rc, &rm, var) ){
          if(lm && rm){ 
            //eep.  We're trying to multiply the variable with itself... 
            //We don't handle powers yet, though this would be the place to implement it.
            return false;
          }
          *m += lm * rc + rm * lc;
          *c += rc * lc;
          return true;
        }
        return false;
      }
    case PIP_EQN_ADD:
      return
        pip_eqn_cmpnt_simplify_1var(base, cmp->val.branch.ptr_left,  c, m, var) &&
        pip_eqn_cmpnt_simplify_1var(base, cmp->val.branch.ptr_right, c, m, var);
    case PIP_EQN_NEGA:
      {
        float8 pm = 0.0, pc = 0.0;
        if(pip_eqn_cmpnt_simplify_1var(base, cmp->val.ptr, &pc, &pm, var)){
          *m = 0.0 - pm;
          *c = 0.0 - pc;
          return true;
        }
        return false;
      }
    case PIP_EQN_CNSTRT:
      //These just muck things up entirely.  Nothing to do but fail.
      return false;
    default:
      elog(NOTICE, "Unhandled equation component type: %d (%s(); %s:%d)", cmp->type, __FUNCTION__, __FILE__, __LINE__);
  }
  return false;
}


