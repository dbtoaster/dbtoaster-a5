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
#include "pvar.h"

int pip_var_id_parse(char *str, pip_var_id *id)
{
  long group, var;
  int local_len;
  if(sscanf(str, "[%ld:%ld]%n", &group, &var, &local_len) != 3) return -1;
  id->group = group;
  id->variable = var;
  return local_len;
}


int pip_var_parse(char *str, pip_var **pvar)
{
  long     group, variable, subscript;
  int      c, len = 0;
  char    *clone;
  
  *pvar = NULL;
  
  if(sscanf(str, "[%ld:%ld~%ld=>%n", &group, &variable, &subscript, &c) != 4){ 
    if(sscanf(str, "[%ld:%ld=>%n", &group, &variable, &c) != 3){ 
      return -1; 
    }
    subscript = -1;
  }
  if(group >= pip_distribution_count){ return -1; }
  str += c; len += c;

  clone = strstr(str, "]");
  if(clone == NULL){ return -1; }
  c = clone - str;

  clone = palloc0(c + 1);
  strncpy(clone, str, c);
  len += c+1;
  
  (*pvar) = SPI_palloc(PVAR_SIZE(group));
  bzero((*pvar), PVAR_SIZE(group));
  SET_VARSIZE((*pvar), PVAR_SIZE(group));
  
  (*pvar)->vid.variable = variable;
  (*pvar)->vid.group    = group;
  (*pvar)->subscript    = subscript;
  len += pip_var_parse_params(clone, (*pvar));
  
  return len;
}
int pip_var_parse_params(char *str, pip_var *pvar)
{
  if(pip_distributions[pvar->vid.group]->in != NULL){
    return pip_distributions[pvar->vid.group]->in(pvar, str);
  }
  return 0;
}
int pip_var_id_sprint(char *str, int len, pip_var_id *pvar)
{
  int c = 0;
  c += snprintf(str+c, len-c, "[%ld:", (long)pvar->group);
  c += snprintf(str+c, len-c, "%ld]",  (long)pvar->variable);
  return c;
}
int pip_var_sprint(char *str, int len, pip_var *pvar)
{
  int c = 0;
  c += snprintf(str+c, len-c, "[%ld:", (long)pvar->vid.group);
  if(pvar->subscript >= 0){
    c += snprintf(str+c, len-c, "%ld~", (long)pvar->vid.variable);
    c += snprintf(str+c, len-c, "%ld=>", (long)pvar->subscript);
  } else {
    c += snprintf(str+c, len-c, "%ld=>", (long)pvar->vid.variable);
  }
  if(pip_distributions[pvar->vid.group]->out != NULL){
    c += pip_distributions[pvar->vid.group]->out(pvar, len-c, str+c);
  }
  if(c < len+1){
    str[c] = ']';
    str[c+1] = '\0';
  }
  return c+1;
}
bool   pip_var_id_eq(pip_var_id *a, pip_var_id *b)
{
  return (a->group == b->group) && (a->variable == b->variable);
}
bool   pip_var_eq(pip_var *a, pip_var *b)
{
  return pip_var_id_eq(&a->vid, &b->vid);
}
int pip_variable_sort(pip_var *a, pip_var *b)
{
  if(a->vid.group > b->vid.group){
    return 1;
  } else if(a->vid.group < b->vid.group){
    return -1;
  } else if(a->vid.variable > b->vid.variable){
    return 1;
  } else if(a->vid.variable < b->vid.variable){
    return -1;
  } else {
    return 0;
  }
}

float8 pip_var_gen(pip_var *var)
{
  return pip_var_gen_wseed(var, random());
}
float8 pip_var_gen_w_name_and_seed(pip_var *var, int64 seed)
{
  return pip_var_gen_wseed(var, seed ^ pip_prng_step(var->vid.variable));
}
float8 pip_var_gen_wseed(pip_var *var, int64 seed)
{
  if((var->vid.group >= pip_distribution_count) || (var->vid.group < 0)){
    return NAN;
  }
  return pip_distributions[var->vid.group]->gen(var, seed);
}
float8 pip_var_gen_w_range(pip_var *var, float8 low, float8 high)
{
  int64 seed = random();
  if((low < 0.0) || (high > 1.0) || (low > high)){
    return NAN;
  }
  return pip_var_icdf(var, (pip_prng_float(&seed) * (high-low) + low));
}

float8 pip_var_pdf(pip_var *var, float8 point)
{
  if((var->vid.group >= pip_distribution_count) || (var->vid.group < 0)){
    return NAN;
  }
  if(pip_distributions[var->vid.group]->pdf != NULL)
    return pip_distributions[var->vid.group]->pdf(var, point);
  return NAN;
}

float8 pip_var_cdf(pip_var *var, float8 point)
{
  if((var->vid.group >= pip_distribution_count) || (var->vid.group < 0)){
    return NAN;
  }
  if(pip_distributions[var->vid.group]->cdf != NULL)
    return pip_distributions[var->vid.group]->cdf(var, point);
  return NAN;
}

float8 pip_var_icdf(pip_var *var, float8 point)
{
  if((var->vid.group >= pip_distribution_count) || (var->vid.group < 0)){
    return NAN;
  }
  if(pip_distributions[var->vid.group]->icdf != NULL)
    return pip_distributions[var->vid.group]->icdf(var, point);
  return NAN;
}

int pip_group_lookup(char *name)
{
  int i;
  for(i = 0; i < pip_distribution_count; i++){
    if(strcmp(pip_distributions[i]->name, name) == 0) break;
  }
  return (i < pip_distribution_count) ? i : -1;
}

