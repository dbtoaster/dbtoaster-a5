#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "eqn.h"

Datum   pip_exp_in (PG_FUNCTION_ARGS)
{
  char     *str = PG_GETARG_CSTRING(0);
  pip_exp  *exp;
  int       size = 0;
  
  pip_eqn_cmpnt_parse(str+3, NULL, 0, &size);
  size += sizeof(pip_exp);
  
  exp = SPI_palloc(size);
  bzero(exp, size);
  SET_VARSIZE(exp, size);
  exp->type = PIP_EXP_EQN;
  SET_VARSIZE(&exp->val.eqn, size - sizeof(pip_exp) + sizeof(pip_eqn));
  
  pip_eqn_cmpnt_parse(str+3, exp->val.eqn.data, 0, NULL);
  
  PG_RETURN_POINTER(exp);
}

Datum   pip_exp_out (PG_FUNCTION_ARGS)
{
  pip_exp  *exp = (pip_exp *)PG_GETARG_BYTEA_P(0);
  char     *str;
  int       len;
  
  
  str = palloc(508);
  if(exp->type == PIP_EXP_EQN){
    len = pip_eqn_cmpnt_sprint(str+3, 500, exp->val.eqn.data, 0);
    str[0] = '<';
    str[1] = '<';
    str[2] = ' ';
    str[len+1] = ' ';
    str[len+2] = '>';
    str[len+3] = '>';
    str[len+4] = '\0';
  } else if(exp->type == PIP_EXP_FIX) {
    snprintf(str, 505, "%lf", (double)exp->val.fix);
  }
  
  PG_RETURN_CSTRING(str);
}

Datum  pip_exp_make (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  pip_exp  *exp;
  int       size;
  
  size = VARSIZE(eqn) - sizeof(pip_eqn) + sizeof(pip_exp);
  exp = SPI_palloc(size);
  bzero(exp, size);
  SET_VARSIZE(exp, size);
  exp->type = PIP_EXP_EQN;
  memcpy(&exp->val.eqn, eqn, VARSIZE(eqn));
  
  PG_RETURN_POINTER(exp);
}
Datum  pip_exp_fix (PG_FUNCTION_ARGS)
{
  pip_exp         *exp = (pip_exp *)PG_GETARG_BYTEA_P(0);
  HeapTupleHeader  row = PG_GETARG_HEAPTUPLEHEADER(1);
  pip_exp         *ret;
  float8           result;
  int              atom_count = 0;
  pip_atom       **atoms = NULL;
  
  SPI_connect();
  atom_count = pip_extract_clause(row, &atoms);
  result = pip_compute_expectation(&exp->val.eqn, atom_count, atoms, 1000);
  SPI_finish();

  ret = palloc0(sizeof(pip_exp));
  ret->type = PIP_EXP_FIX;
  ret->val.fix = result;
  
  PG_RETURN_POINTER(ret);
}
Datum  pip_exp_expect (PG_FUNCTION_ARGS)
{
  pip_exp   *exp = (pip_exp *)PG_GETARG_BYTEA_P(0);
  float8     result = 0.0;
  
  switch(exp->type){
    case PIP_EXP_FIX:
      result = exp->val.fix;
      break;
    case PIP_EXP_EQN:
      result = pip_compute_expectation_conditionless(&exp->val.eqn, 1000);
      break;
  }
  
  PG_RETURN_FLOAT8(result);
}
