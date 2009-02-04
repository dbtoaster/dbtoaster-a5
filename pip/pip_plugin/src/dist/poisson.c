#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include "postgres.h"			/* general Postgres declarations */
#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */
#include "executor/executor.h"
#include "pip.h"
#include "catalog/pg_type.h"

static float8 gammln(const float8 xx);  
float8  pip_poisson(float8 lambda, int64 *seed);
void    pip_poisson_init (pip_var *var, HeapTupleHeader params);
float8  pip_poisson_gen  (pip_var *var, int64 seed);
float8  pip_poisson_pdf  (pip_var *var, float8 point);
int pip_poisson_in   (pip_var *var, char *str);
int pip_poisson_out  (pip_var *var, int len, char *str);

DECLARE_PIP_DISTRIBUTION(poisson) = {
  .name = "Poisson",
  .size = sizeof(float8),
  .init= &pip_poisson_init,
  .gen = &pip_poisson_gen,
  .pdf = &pip_poisson_pdf,
  .cdf = NULL,
  .icdf= NULL,
  .in  = &pip_poisson_in,
  .out = &pip_poisson_out,
  .joint= false
};


static float8 gamma_cof[] = {
  57.1562356658629235,
 -59.5979603554754912,
  14.1360979747417471,
  -0.491913816097620199,
    .339946499848118887e-4,
    .465236289270485756e-4,
   -.983744753048795646e-4,
    .158088703224912494e-3,
   -.210264441724104883e-3,
    .217439618115212643e-3,
   -.164318106536763890e-3,
    .844182239838527433e-4,
   -.261908384015814087e-4,
    .368991826595316234e-5
};

//NR Section 6.1
static float8 gammln(const float8 xx)
{
  int j;
  float8 x, tmp, y, ser;
  if(xx <= 0) return NAN;
  y = x = xx;
  tmp = x + 5.2421875; //Rational 671/128
  tmp = (x + 0.5) * log(tmp) - tmp;
  ser = 0.999999999999997092;
  for(j = 0; j < 14; j++) ser += gamma_cof[j]/++y;
  return tmp + log(2.5066282746310005 * ser / x);
}

//Knuth's Algorithm
float8
pip_poisson(float8 lambda, int64 *seed)
{
  float8 k = 0, p = 1, L = exp(-lambda);
  
  while(p >= L){
    k += 1.0;
    p *= pip_prng_float(seed);
  }
  return k;
}

void    pip_poisson_init (pip_var *var, HeapTupleHeader params)
{
  *((float8 *)var->group_state) = dist_param_float8(params, 0, 0.0);
}
float8  pip_poisson_gen  (pip_var *var, int64 seed)
{
  return pip_poisson(*((float8 *)var->group_state), &seed);
}
float8  pip_poisson_pdf  (pip_var *var, float8 point)
{
  if(point < 0) return NAN;
  point = floor(point);
  return exp(-(double)((float8 *)var->group_state)[0] + (double)point * log((double)((float8 *)var->group_state)[0]) - gammln(point + 1));
}
int pip_poisson_in   (pip_var *var, char *str)
{
  double lambda;
  int c;
  
  sscanf(str, "L=%lf%n", &lambda, &c);
  
  ((float8 *)var->group_state)[0] = lambda;
  return c;
}
int pip_poisson_out  (pip_var *var, int len, char *str)
{
  return snprintf(str, len, "L=%lf", (double)((float8 *)var->group_state)[0]);
}

