//////////////////////////////////////////////////////////////////////////
// sample_set.c                                                        
// 
// Sample sets are used for two things
//  - Storing a compressed representation of variable-value mappings for
//    a fixed number of possible worlds.  The default representation of
//    a sample set includes only an integer seed that is combined with
//    the variable name and passed to the generator to produce a value
//    deterministically.  Specific variables may be singled out to be
//    defined with specific values.
//  - Storing a vector of values.  This is used when aggregating
//    histograms.
// 
//////////////////////////////////////////////////////////////////////////

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
#include "pvar.h"
#include "eqn.h"
#include "sample_set.h"

int  pip_sample_var_to_id (pip_sample_set *set, pip_var_id *var);
void pip_sample_seed_set  (pip_sample_set *set, int sample, int64 val);

/******************** Variable Operations **********************/

int64 pip_global_ssid = 0; //sample sets are not persistent, so we can loop.

#define SAMPLE_SET_ENTRY(id) ((pip_sample_mapping *)(set->data + (id) * (sizeof(pip_sample_mapping) + sizeof(float8) * set->sample_cnt)))

pip_sample_set *pip_sample_set_create(int32 sample_count, int32 var_count)
{
  size_t varlen =
    sizeof(pip_sample_set) + 
    ( sizeof(pip_sample_mapping) + sizeof(float8) * sample_count ) * var_count;
  //Allocate with the SPI-aware palloc in case we want to
  //return the sample set.  Since sample_set is a SQL type, 
  //this will typically be the case; and if SPI isn't
  //connected, then this acts as a normal palloc.
  pip_sample_set *set = SPI_palloc(varlen);
  int i, j; 
  
  SET_VARSIZE(set, varlen);
  set->var_cnt = var_count;
  set->sample_cnt = sample_count;
  set->ssid = pip_global_ssid++;
  set->seed = (int64)random();
  
  //bzero(set->data, ( sizeof(pip_sample_mapping) + sizeof(float8) * sample_count ) * var_count);
  for(i = 0; i < var_count; i++){
    for(j = 0; j < sample_count; j++){
      SAMPLE_SET_ENTRY(i)->val[j] = NAN;
    }
  }
  
  elog(PIP_PRESAMPLE_LOGLEVEL, "Creating pip sample set: %ds, %dv -> allocated %d", sample_count, var_count, (int)varlen);
  return set;
}

int       pip_sample_var_to_id     (pip_sample_set *set, pip_var_id *var)
{
  int i;
  for(i = 0; i < set->var_cnt; i++){
    if(pip_var_id_eq(&SAMPLE_SET_ENTRY(i)->var, var)) return i;
  }
  return -1;
}
void      pip_sample_name_set      (pip_sample_set *set, int var, pip_var_id *name)
{
  SAMPLE_SET_ENTRY(var)->var = *name;
}
pip_var_id *pip_sample_name_get      (pip_sample_set *set, int var)
{
  return &SAMPLE_SET_ENTRY(var)->var;
}
bool      pip_sample_name_is       (pip_sample_set *set, int var, pip_var_id *name)
{
  return pip_var_id_eq(&SAMPLE_SET_ENTRY(var)->var, name);
}
void      pip_sample_val_set_by_id (pip_sample_set *set, int var, int sample, float8 val)
{
  SAMPLE_SET_ENTRY(var)->val[sample] = val;
}
void      pip_sample_val_set       (pip_sample_set *set, pip_var_id *var, int sample, float8 val)
{
  int id = pip_sample_var_to_id(set, var);
  if(id >= 0) pip_sample_val_set_by_id(set, id, sample, val);
}
float8    pip_sample_val_get_by_id (pip_sample_set *set, int var, int sample)
{
  return SAMPLE_SET_ENTRY(var)->val[sample];
}
float8    pip_sample_val_get       (pip_sample_set *set, pip_var_id *var, int sample)
{
  int id = pip_sample_var_to_id(set, var);
  if(id >= 0) return pip_sample_val_get_by_id(set, id, sample);
  return NAN;
}
int64     pip_sample_seed          (pip_sample_set *set, int sample)
{
  return pip_prng_step(set->seed ^ pip_prng_step(sample));
}

float8 pip_sample_var_val(pip_sample_set *set, int sample, pip_var *var)
{
  float8 val; bool islocal = true;
  val = pip_sample_val_get(set, &var->vid, sample);
  if(isnan(val)){
    int seed = pip_sample_seed(set,sample);
    islocal = false;
    val = pip_var_gen_w_name_and_seed(var, seed);
  }
  return val;
}

pip_sample_set *pip_sample_set_vector_max (pip_eqn *eqn, pip_sample_set *set, int clause_cnt, pip_atom **clause)
{
  int i, j;
  for(i = 0; i < set->sample_cnt; i++){
    float8 val;
    for(j = 0; j < clause_cnt; j++){
      if(!pip_atom_evaluate_seed(clause[j], pip_sample_seed(set, i))) break;
    }
    if(j >= clause_cnt){
      val = pip_eqn_evaluate_seed(eqn, pip_sample_seed(set, i));
      if(isnan(SAMPLE_SET_ENTRY(0)->val[i]) || (SAMPLE_SET_ENTRY(0)->val[i] < val)){
        SAMPLE_SET_ENTRY(0)->val[i] = val;
      }
    } else if(isnan(SAMPLE_SET_ENTRY(0)->val[i])){
      SAMPLE_SET_ENTRY(0)->val[i] = 0;
    }
  }
  return set;
}
pip_sample_set *pip_sample_set_vector_sum (pip_eqn *eqn, pip_sample_set *set, int clause_cnt, pip_atom **clause)
{
  int i, j;
  for(i = 0; i < set->sample_cnt; i++){
    float8 val;
    for(j = 0; j < clause_cnt; j++){
      if(!pip_atom_evaluate_seed(clause[j], pip_sample_seed(set, i))) break;
    }
    if(j >= clause_cnt){
      val = pip_eqn_evaluate_seed(eqn, pip_sample_seed(set, i));
      if(isnan(SAMPLE_SET_ENTRY(0)->val[i])){
        SAMPLE_SET_ENTRY(0)->val[i] = val;    
      } else {
        SAMPLE_SET_ENTRY(0)->val[i] += val;
      }
    } else if(isnan(SAMPLE_SET_ENTRY(0)->val[i])){
      SAMPLE_SET_ENTRY(0)->val[i] = 0;
    }
  }
  return set;
}
