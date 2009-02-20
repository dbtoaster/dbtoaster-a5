#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include "postgres.h"			/* general Postgres declarations */
#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */
#include "executor/executor.h"
#include "catalog/pg_type.h"  

#include "pip.h"

void    pip_exponential_init (pip_var *var, HeapTupleHeader params);
float8  pip_exponential_gen  (pip_var *var, int64 seed);
float8  pip_exponential_pdf  (pip_var *var, float8 point);
float8  pip_exponential_cdf  (pip_var *var, float8 point);
float8  pip_exponential_icdf (pip_var *var, float8 point);
int     pip_exponential_in   (pip_var *var, char *str);
int     pip_exponential_out  (pip_var *var, int len, char *str);

DECLARE_PIP_DISTRIBUTION(exponential) = {
  .name = "Exponential",
  .size = sizeof(float8),
  .init= &pip_exponential_init,
  .gen = &pip_exponential_gen,
  .pdf = &pip_exponential_pdf,
  .cdf = &pip_exponential_cdf,
  .icdf= &pip_exponential_icdf,
  .in  = &pip_exponential_in,
  .out = &pip_exponential_out,
  .joint= false
};

void    pip_exponential_init (pip_var *var, HeapTupleHeader params)
{
  ((float8 *)var->group_state)[0] = dist_param_float8(params, 0, 1.0);
}
float8  pip_exponential_gen  (pip_var *var, int64 seed)
{
  return pip_exponential_icdf(var, pip_prng_float(&seed));
}
float8  pip_exponential_pdf  (pip_var *var, float8 point)
{
  return ((float8 *)var->group_state)[0] * exp(- point * ((float8 *)var->group_state)[0]);
}
float8  pip_exponential_cdf  (pip_var *var, float8 point)
{
  return 1 - exp(- point * ((float8 *)var->group_state)[0]);
}
float8  pip_exponential_icdf (pip_var *var, float8 point)
{
  return - log(1 - point) / ((float8 *)var->group_state)[0];
}
int pip_exponential_in   (pip_var *var, char *str)
{
  double lambda;
  int c;
  
  sscanf(str, "%lf%n", &lambda, &c);
  
  ((float8 *)var->group_state)[0] = lambda;
  return c;
}
int pip_exponential_out  (pip_var *var, int len, char *str)
{
  return snprintf(str, len, "%lf", (double)((float8 *)var->group_state)[0]);
}




