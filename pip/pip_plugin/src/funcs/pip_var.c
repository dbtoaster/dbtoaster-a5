#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "pvar.h"
#include "eqn.h"

Datum   pip_var_in   (PG_FUNCTION_ARGS)
{
  char     *str       = PG_GETARG_CSTRING(0);
  pip_var  *result = NULL;
  
  pip_var_parse(str, &result);
  
  if(!result){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for pvar: \"%s\"",
                    str)));
  }
  PG_RETURN_POINTER(result);
}
Datum   pip_var_out  (PG_FUNCTION_ARGS)
{
  pip_var  *pvar = (pip_var *) PG_GETARG_BYTEA_P(0);
  char     *result;
  
  result = (char *) palloc(200);
  
  pip_var_sprint(result, 200, pvar);
  
  PG_RETURN_CSTRING(result);
}
Datum   pip_var_create_str (PG_FUNCTION_ARGS)
{
  pip_var    *pvar;
  char       *group = PG_GETARG_CSTRING(0);
  int64       variable = PG_GETARG_INT32(1);
  char       *params = PG_GETARG_CSTRING(2);
  int         group_id = pip_group_lookup(group);
  
  if(group_id < 0){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("Unknown distribution type: \"%s\"",
                    group)));
  }
  
  pvar = SPI_palloc(PVAR_SIZE(group_id));
  bzero(pvar, PVAR_SIZE(group_id));
  SET_VARSIZE(pvar, PVAR_SIZE(group_id));
  
  pvar->vid.group = group_id;
  pvar->vid.variable = variable;

  pip_var_parse_params(params, pvar);
    
  PG_RETURN_POINTER(pip_eqn_for_var(pvar));
}
Datum   pip_var_create_row (PG_FUNCTION_ARGS)
{
  pip_var        *pvar;
  char           *group = PG_GETARG_CSTRING(0);
  int64           variable = PG_GETARG_INT32(1);
  HeapTupleHeader params = PG_GETARG_HEAPTUPLEHEADER(2);
  int             group_id = pip_group_lookup(group);

  if(group_id < 0){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("Unknown distribution type: \"%s\"",
                    group)));
  }
  
  pvar = SPI_palloc(PVAR_SIZE(group_id));
  bzero(pvar, PVAR_SIZE(group_id));
  SET_VARSIZE(pvar, PVAR_SIZE(group_id));
  
  pvar->vid.group = group_id;
  pvar->vid.variable = variable;

  if(pip_distributions[group_id]->init){
    pip_distributions[group_id]->init(pvar, params);
  }
    
  PG_RETURN_POINTER(pip_eqn_for_var(pvar));
}
