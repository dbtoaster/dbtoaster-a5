#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "value_bundle.h"

Datum   pip_value_bundle_in   (PG_FUNCTION_ARGS)
{
  char             *str = PG_GETARG_CSTRING(0);
  int               c, worldcount;
  long              seed;
  pip_var          *var = NULL;
  pip_value_bundle *valbundle;
  
  SPI_connect();
  
  if(sscanf(str, "%ld;%d;%n", &seed, &worldcount, &c) != 3){
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("value bundle, can't load seed!")));
  }
  
  if(str[c] == '\0'){
    var = NULL;
  } else {
    pip_var_parse(str+c, &var);
    if(!var){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("value bundle, can't load variable")));
    }
  }
  
  valbundle = pip_value_bundle_alloc(var, worldcount, seed);

  SPI_finish();

  PG_RETURN_POINTER(valbundle);
}
Datum   pip_value_bundle_out  (PG_FUNCTION_ARGS)
{
  pip_value_bundle *valbundle = (pip_value_bundle *)PG_GETARG_BYTEA_P(0);
  char             *result;
  int               c;
  
  result = (char *) palloc(100);
  
  //the value bundle is reproducible from the initial seed value;
  //just save the base things.
  c = snprintf(result, 100, "%ld;%d;", 
    (long)valbundle->seed, valbundle->worldcount);
  pip_var_sprint(result+c, 100-c, PIP_VB_VAR(valbundle));

  PG_RETURN_CSTRING(result);
}

Datum   pip_value_bundle_create  (PG_FUNCTION_ARGS)
{
  pip_eqn          *eqn = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  int               worldcount = PG_GETARG_INT32(1);
  pip_var          *var;
  pip_value_bundle *valbundle;
  
  SPI_connect();
  var = pip_var_for_eqn(eqn);
  if(var == NULL) elog(ERROR, "Error, invalid input to value_bundle_create; Full eqn support is not implemented");
  valbundle = pip_value_bundle_alloc(var, worldcount, random());

  SPI_finish();

  PG_RETURN_POINTER(valbundle);
}

#define UNSET_WORLDBIT(i) wp->data[i/8] &= ((~(1 << (7-(i%8))))&0xff)
#define MIN(a,b) ((a > b) ? (b) : (a))

#define CMP_FUNC(left_val,right_val) \
  pip_world_presence *wp = (pip_world_presence *)PG_GETARG_BYTEA_P_COPY(0);\
  int                 i;\
  for(i = 0; i < wp->worldcount; i++) { if(left_val <= right_val) { UNSET_WORLDBIT(i); }}\
  PG_RETURN_POINTER(wp)

Datum   pip_value_bundle_cmp_vv  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *val_left = (pip_value_bundle *)PG_GETARG_BYTEA_P(1);
  pip_value_bundle   *val_right = (pip_value_bundle *)PG_GETARG_BYTEA_P(2);
  CMP_FUNC(PIP_VB_VAL(val_left)[i], PIP_VB_VAL(val_right)[i]);
}
Datum   pip_value_bundle_cmp_vi  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *val_left = (pip_value_bundle *)PG_GETARG_BYTEA_P(1);
  int32               val_right = PG_GETARG_INT32(2);
  CMP_FUNC(PIP_VB_VAL(val_left)[i], val_right);
}
Datum   pip_value_bundle_cmp_iv  (PG_FUNCTION_ARGS)
{
  int32               val_left = PG_GETARG_INT32(1);
  pip_value_bundle   *val_right = (pip_value_bundle *)PG_GETARG_BYTEA_P(2);
  CMP_FUNC(val_left, PIP_VB_VAL(val_right)[i]);
}
Datum   pip_value_bundle_cmp_vf  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *val_left = (pip_value_bundle *)PG_GETARG_BYTEA_P(1);
  float8              val_right = PG_GETARG_FLOAT8(2);
  CMP_FUNC(PIP_VB_VAL(val_left)[i], val_right);
}
Datum   pip_value_bundle_cmp_fv  (PG_FUNCTION_ARGS)
{
  float8              val_left = PG_GETARG_FLOAT8(1);
  pip_value_bundle   *val_right = (pip_value_bundle *)PG_GETARG_BYTEA_P(2);
  CMP_FUNC(val_left, PIP_VB_VAL(val_right)[i]);
}

#define UPDATE_FUNC(result_comp, worldcount) \
  pip_value_bundle   *result;\
  int i;\
  result = pip_value_bundle_alloc(NULL, worldcount, 0);\
  for(i = 0; i < worldcount; i++){\
    PIP_VB_VAL(result)[i] = (result_comp);\
  }\
  PG_RETURN_POINTER(result)

Datum   pip_value_bundle_add_vf  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *valbundle = (pip_value_bundle *)PG_GETARG_BYTEA_P(0);
  float8              cmp = PG_GETARG_FLOAT8(1);
  
  UPDATE_FUNC((PIP_VB_VAL(valbundle)[i] + cmp), valbundle->worldcount);
}
Datum   pip_value_bundle_add_vv  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *valbundle_l = (PG_ARGISNULL(0)) ? NULL : (pip_value_bundle *)PG_GETARG_BYTEA_P(0);
  pip_value_bundle   *valbundle_r = (PG_ARGISNULL(1)) ? NULL : (pip_value_bundle *)PG_GETARG_BYTEA_P(1);
  //since this operation is also used for aggregation we need a WP option.
  pip_world_presence *wp = ((PG_NARGS() > 2) && (!PG_ARGISNULL(2))) ? ((pip_world_presence *)PG_GETARG_BYTEA_P(2)) : (NULL);

  if(valbundle_r){
    if(!valbundle_l){
      valbundle_l = pip_value_bundle_alloc(NULL, valbundle_r->worldcount, 0);
    }
  } else {
    if(!valbundle_l){
      PG_RETURN_NULL();
    }
  }

  {
    UPDATE_FUNC(
      (!wp || ((wp->data[i/8] >> (7-(i%8))) & 0x01)) ? 
        (PIP_VB_VAL(valbundle_l)[i] + PIP_VB_VAL(valbundle_r)[i]) : 
        PIP_VB_VAL(valbundle_l)[i],
      valbundle_l->worldcount
    );
  }
}
Datum   pip_value_bundle_mul_vf  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *valbundle = (pip_value_bundle *)PG_GETARG_BYTEA_P(0);
  float8              cmp = PG_GETARG_FLOAT8(1);
  
  UPDATE_FUNC((PIP_VB_VAL(valbundle)[i] * cmp), valbundle->worldcount);
}
Datum   pip_value_bundle_mul_vv  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *valbundle_l = (pip_value_bundle *)PG_GETARG_BYTEA_P(0);
  pip_value_bundle   *valbundle_r = (pip_value_bundle *)PG_GETARG_BYTEA_P(1);
  
  UPDATE_FUNC((PIP_VB_VAL(valbundle_l)[i] * PIP_VB_VAL(valbundle_r)[i]), valbundle_l->worldcount);
}
Datum   pip_value_bundle_expect  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *valbundle = (pip_value_bundle *)PG_GETARG_BYTEA_P(0);
  pip_world_presence *wp = (fcinfo->nargs == 2) ? ((pip_world_presence *)PG_GETARG_BYTEA_P(1)) : (NULL);
  int                 low  = (fcinfo->nargs == 3) ? (PG_GETARG_INT32(1)) : (0);
  int                 high = (fcinfo->nargs == 3) ? (PG_GETARG_INT32(2)) : (valbundle->worldcount);
  float8              result = 0;
  int i, cnt = 0;

  for(i = low; i < high; i++){
    if(!wp || ((wp->data[i/8] >> (7-(i%8))) & 0x01)){
      result += PIP_VB_VAL(valbundle)[i];
      cnt++;
    }
  }
  
  if(cnt > 0)
    result /= high;
  
  PG_RETURN_FLOAT8(result);
}
Datum   pip_value_bundle_max  (PG_FUNCTION_ARGS)
{
  pip_value_bundle   *left = (pip_value_bundle *)PG_GETARG_BYTEA_P(0);
  pip_value_bundle   *right = (pip_value_bundle *)PG_GETARG_BYTEA_P(1);
  int i;
  
  for(i = 0; i < left->worldcount; i++){
    if(PIP_VB_VAL(left)[i] < PIP_VB_VAL(right)[i]){
      PIP_VB_VAL(left)[i] = PIP_VB_VAL(right)[i];
    }
  }
  
  PG_RETURN_POINTER(left);
}
