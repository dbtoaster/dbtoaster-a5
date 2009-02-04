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
#include "value_bundle.h"


pip_value_bundle *pip_value_bundle_alloc(pip_var *var, int worldcount, int64 seed)
{
  int i;
  size_t size = sizeof(pip_value_bundle) + sizeof(float8) * worldcount + ((var == NULL) ? (sizeof(pip_var)) : VARSIZE(var));
  pip_value_bundle *valbundle;
  
  elog(PIP_MCDB_INFO_LOGLEVEL, "Allocating %d bytes for value bundle [%d:%d] (worldcount: %d)", 
    (int)size, 
    (var == NULL) ? (-1) : ((int)var->vid.group),
    (var == NULL) ? (-1) : ((int)var->vid.variable), 
    worldcount);
  
  valbundle = SPI_palloc(size);
  bzero(valbundle, size);
  SET_VARSIZE(valbundle, size);
  
  valbundle->worldcount = worldcount;
  valbundle->seed = seed;
  if(var == NULL){
    bzero(PIP_VB_VAR(valbundle), sizeof(pip_var));
    SET_VARSIZE(PIP_VB_VAR(valbundle), sizeof(pip_var));
  } else {
    memcpy(PIP_VB_VAR(valbundle), var, VARSIZE(var));
    for(i = 0; i < worldcount; i++){
      PIP_VB_VAL(valbundle)[i] = pip_var_gen_wseed(PIP_VB_VAR(valbundle), seed ^ (pip_prng_step(i) << 32));
    }
  }
  
  return valbundle;
}

