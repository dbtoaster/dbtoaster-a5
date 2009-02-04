#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"

Datum   pip_presampler_in(PG_FUNCTION_ARGS)
{
  char                 *str = PG_GETARG_CSTRING(0);
  long                  val,path, curr_count;
  int                   i,c;
  int64                 trackback[32];
  pip_sample_generator *result;
  
  if(sscanf(str, "%ld:%ld|%n", &path, &curr_count, &c) != 3){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("text representation WRONG! (psin1)")));
  }
  str+=c;
  for(i = 0; i < 32; i++){
    if(sscanf(str, "%ld:%n", &val, &c) != 2){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("text representation WRONG! (psin2:%d)",i)));
    }
    trackback[i] = val;
    str+=c;
  }
  
  result = palloc(sizeof(pip_sample_generator));
  pip_presample_tree_load(result, trackback, path, curr_count);
  PG_RETURN_POINTER(result);
}
Datum   pip_presampler_out(PG_FUNCTION_ARGS)
{
  pip_sample_generator *input = (pip_sample_generator *)PG_GETARG_POINTER(0);
  char                 *output = palloc(550);
  int64                 path, curr_count;
  int64                 trackback[32];
  int                   c = 0, i;

  pip_presample_tree_save(input, trackback, &path, &curr_count);
  
  c = snprintf(output, 550, "%ld:%ld|", (long)path, (long)curr_count);
  for(i = 0; i < 32; i++){
    c += snprintf(output+c, 550-c, "%ld:", (long)trackback[i]); 
  }
  PG_RETURN_CSTRING(output);
}

Datum   pip_presampler_create(PG_FUNCTION_ARGS)
{
  int64                 size = PG_GETARG_INT32(0);
  pip_sample_generator *result;
  
  elog(PIP_SAMPLE_LOGLEVEL, "Sample Tree Being Created"); 
  result = palloc(sizeof(pip_sample_generator));
  pip_presample_tree_init(result, size);
  elog(PIP_SAMPLE_LOGLEVEL, "Sample Tree Initialized"); 
  
  PG_RETURN_POINTER(result);
}
Datum   pip_presampler_advance(PG_FUNCTION_ARGS)
{
  pip_sample_generator *input = (pip_sample_generator *)PG_GETARG_POINTER(0), *output;
  bool isdone;
  int nextid;
  
  output = palloc(sizeof(pip_sample_generator));
  memcpy(output, input, sizeof(pip_sample_generator));
  nextid = pip_presample_tree_next(output, &isdone);
  elog(PIP_SAMPLE_LOGLEVEL, "Advanced sampler to: %d", nextid); 
  
  if(isdone){
    pfree(output);
    PG_RETURN_NULL();
  }
  PG_RETURN_POINTER(output);
}
Datum   pip_presampler_get(PG_FUNCTION_ARGS)
{
  pip_sample_generator *sampler = (pip_sample_generator *)PG_GETARG_POINTER(0);
  PG_RETURN_FLOAT8(pip_presample_tree_curr_float(sampler));
}
