#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "sample_set.h"

#define FIND_NEXT(needle) do {\
  str = tmp;\
  tmp = strstr(str, (needle));\
  if(tmp == NULL) goto error;\
  *tmp = '\0';\
  tmp++;\
  } while(0)

//FORMAT: "SIZE/VARCNT{SEED/SEED/...}[VAR]{VALUE/VALUE/...}..."

Datum   pip_sample_set_in(PG_FUNCTION_ARGS)
{
  char           *str = PG_GETARG_CSTRING(0);
  int             i, j, len = strlen(str);
  char           *str2 = NULL, *tmp;
  int32           sample_count, var_count;
  long            ssid, seed;
  pip_var_id      vid;
  double          value;
  pip_sample_set *result = NULL;
  
  if(str[0] == '?'){
    if(sscanf(str+1, "%d/%d", &sample_count, &var_count) != 2) goto error;
    result = pip_sample_set_create(sample_count, var_count);
  } else {
    str2 = (char *)palloc(len+1);
    strcpy(str2, str);
    tmp = str2;
    
    FIND_NEXT(":");
    if(sscanf(str, "%ld/%d/%d/%ld", &ssid, &sample_count, &var_count, &seed) != 3) goto error;
    
    result = pip_sample_set_create(sample_count, var_count);
    result->ssid = ssid;
    result->seed = seed;
    
    for(i = 0; i < var_count; i++){
      FIND_NEXT("{");
      if(pip_var_id_parse(str, &vid) < 0) goto error;
      pip_sample_name_set(result, i, &vid);
      for(j = 0; j < sample_count; j++){
        FIND_NEXT((j==sample_count-1)?"}":"/");
        if(sscanf(str, "%lf", &value) != 1) goto error;
        pip_sample_val_set_by_id(result, i, j, value);
      }
    }
  }
  
  goto finish;
error:
  ereport(ERROR,
          (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
           errmsg("text representation WRONG!  (why are you creating a sample set anyway?)")));
  if(result) SPI_pfree(result);
  result = NULL;
  
finish:
  if(str2) pfree(str2);
  
  if(result) PG_RETURN_POINTER(result);
  else PG_RETURN_NULL();
}

Datum   pip_sample_set_out(PG_FUNCTION_ARGS)
{
  pip_sample_set *input = (pip_sample_set *)PG_GETARG_BYTEA_P(0);
  int             i, j, c;
  int             size = ((15 + 30 * input->sample_cnt) * input->var_cnt + 15 + 20 * input->sample_cnt);
  char           *result = palloc0(size);
  
  c = 0;
  c += snprintf(result+c, size-c, "%ld/%d/%d/%ld:", (long)input->ssid, input->sample_cnt, input->var_cnt, (long)input->seed);

  for(i = 0; i < input->var_cnt; i++){
    result[c] = '{';
    c++;
    c += pip_var_id_sprint(result + c, size - c, pip_sample_name_get(input, i));
    for(j = 0; j < input->sample_cnt; j++){
      c += snprintf(result+c, size-c, "%f%c", (float)pip_sample_val_get_by_id(input, i, j), (j==input->sample_cnt-1)?'}':'/');
    }
  }
  
  PG_RETURN_CSTRING(result);
}

Datum   pip_sample_set_generate(PG_FUNCTION_ARGS)
{
  HeapTupleHeader   row = PG_GETARG_HEAPTUPLEHEADER(0);
  int64             sample_count = PG_GETARG_INT32(1);
  int               atom_count = 0;
  pip_atom        **atoms = NULL;
  pip_sample_set   *samples;
  
  
  if(sample_count > 0){
  
    SPI_connect();
    
    atom_count = pip_extract_clause(row, &atoms);
    samples =    pip_sample_by_clause(atom_count, atoms, sample_count, NULL);
    
    SPI_finish();
    PG_RETURN_POINTER(samples);
    
  } else {
    PG_RETURN_NULL();
  }
}

Datum   pip_sample_set_explode(PG_FUNCTION_ARGS)
{
  pip_sample_set   *set = (pip_sample_set *)PG_GETARG_BYTEA_P(0);
  FuncCallContext  *funcctx;
  
  if(SRF_IS_FIRSTCALL()){
    funcctx = SRF_FIRSTCALL_INIT();
    funcctx->max_calls = set->sample_cnt;
  }
  
  funcctx = SRF_PERCALL_SETUP();
  
  if(funcctx->call_cntr < set->sample_cnt){
    SRF_RETURN_NEXT(funcctx, Float8GetDatum(pip_sample_val_get_by_id(set, 0, funcctx->call_cntr)));
  } else {
    SRF_RETURN_DONE(funcctx);
  }
}
Datum   pip_sample_set_expect(PG_FUNCTION_ARGS)
{
  pip_sample_set   *set = (pip_sample_set *)PG_GETARG_BYTEA_P(0);
  float8 result = 0.0;
  int i;
  
  for(i = 0; i < set->sample_cnt; i++){
    result += pip_sample_val_get_by_id(set, 0, i);
  }
  
  PG_RETURN_FLOAT8(result / (float8)(set->sample_cnt));
}
