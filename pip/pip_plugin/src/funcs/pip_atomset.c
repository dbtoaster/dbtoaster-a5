#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/geo_decls.h"

#include "pip.h"
#include "eqn.h"
#include "atom.h"
#include "atomset.h"

Datum   pip_atomset_in (PG_FUNCTION_ARGS)
{
  char        *str = PG_GETARG_CSTRING(0);
  pip_atomset *atomset = NULL;
  
  if(str[0] = '?'){
    int cachesize = 0;
    if(str[1] != '\0'){
      sscanf(str+1, "%d", &cachesize);
    }
    pip_atomset_create(NULL, 0, NULL, cachesize);
  } else {
    elog(ERROR, "PIP atom parsing has not been implemented yet");
  }
  
  PG_RETURN_POINTER(atom);
}

Datum   pip_atomset_out (PG_FUNCTION_ARGS)
{
  char *dummy = "[pip atomset]";
  char *ret = palloc0(strlen(dummy);
  strcpy(ret, dummy);
  
  PG_RETURN_CSTRING(ret);
}
