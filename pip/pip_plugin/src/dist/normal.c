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

#ifdef NORMAL_CDF_USE_TABLE
#include "normal.dist"
#endif

void    pip_normal_init (pip_var *var, HeapTupleHeader params);
float8  pip_normal_gen  (pip_var *var, int64 seed);
float8  pip_normal_pdf  (pip_var *var, float8 point);
float8  pip_normal_cdf  (pip_var *var, float8 point);
float8  pip_normal_icdf (pip_var *var, float8 point);
int pip_normal_in   (pip_var *var, char *str);
int pip_normal_out  (pip_var *var, int len, char *str);

DECLARE_PIP_DISTRIBUTION(normal) = {
  .name = "Normal",
  .size = sizeof(float8) * 2,
  .init= &pip_normal_init,
  .gen = &pip_normal_gen,
  .pdf = &pip_normal_pdf,
  .cdf = &pip_normal_cdf,
  .icdf= &pip_normal_icdf,
  .in  = &pip_normal_in,
  .out = &pip_normal_out,
  .joint= false
};

void
pip_box_muller(float8 *X, float8 *Y, int64 *seed)
{
  float8          u,v,s;
  
  //The Polar form of the Box-Muller transform generates a pair of normally distributed values.
  //for simplicity, for the time being, we're going to output only one.
  s = 0.0;
  while((s <= 0.0) || (s > 1.0)){
    //u and v are distributed uniformally in the range [-1, 1]
    u = pip_prng_float(seed) * 2.0 - 1.0;
    v = pip_prng_float(seed) * 2.0 - 1.0;
    s = u*u + v*v;
  }

  s = sqrt((-2 * log(s)) / s);

  *X = u * s; 
  *Y = v * s;
  // X and Y now contain independent normally distrubuted values with mean 0 and std_dev 1
}

void    pip_normal_init (pip_var *var, HeapTupleHeader params)
{
  ((float8 *)var->group_state)[0] = dist_param_float8(params, 0, 0.0);
  ((float8 *)var->group_state)[1] = dist_param_float8(params, 1, 1.0);
//  elog(NOTICE, "Making a normal variable: u=%lf, s=%lf", ((float8 *)var->group_state)[0], ((float8 *)var->group_state)[1]);
}
float8  pip_normal_gen  (pip_var *var, int64 seed)
{
  float8 X, Y;
  pip_box_muller(&X, &Y, &seed);
  // value = Normal(0,1) * stddev + mean
  return (X * ((float8 *)var->group_state)[1]) + ((float8 *)var->group_state)[0];  
}
float8  pip_normal_pdf  (pip_var *var, float8 point)
{
  float8 mean   = ((float8 *)var->group_state)[0];
  float8 stddev = ((float8 *)var->group_state)[1];
  return 
    (float8)exp((double)(0 - ( (point - mean) * (point - mean) / ( 2 * stddev * stddev) )))
      /
    ( stddev * 2.506628274631 );
  //2.506628274631 = sqrt(2pi)
}
float8  pip_normal_cdf  (pip_var *var, float8 point)
{
#ifdef NORMAL_CDF_USE_TABLE
  int index;
  double interpolation;
  double result;
#endif

  point -= ((float8 *)var->group_state)[0]; //mean
  point /= ((float8 *)var->group_state)[1]; //stddev
  
#ifndef NORMAL_CDF_USE_TABLE
  return erf(point);
#else
  index = 
    (int)(
      ((point < 0.0) ? (-1.0) : (1.0)) * 
      (point / pip_normal_cdf_precision)
    );
    
  if(index+1 >= pip_normal_cdf_precomp_size){
    //special case the extremities... there's a better formula for this, but
    //this'll do for now.
    result = 
      ((point < 0) ? (-1.0) : 1.0) *
      (0.5 - pip_normal_cdf_precomp[pip_normal_cdf_precomp_size-1]) *
      (pip_normal_cdf_precision * (double)pip_normal_cdf_precomp_size) /
      (double)point;
    result = 0.5 - result;
  } else {
    interpolation = 
      (((point < 0) ? (-point) : point) / 
        pip_normal_cdf_precision) - 
        ((double)index);
    
    result = 
      pip_normal_cdf_precomp[index] * interpolation +
      pip_normal_cdf_precomp[index+1] * (1.0 - interpolation);
  }
  
  return (float8)((point < 0) ? (0.5 - result) : (0.5 + result));
#endif
}

extern double ltqnorm(double p); //library/ldqnorm.c

float8  pip_normal_icdf (pip_var *var, float8 point)
{
#ifndef NORMAL_CDF_USE_TABLE
  return (ltqnorm(point) * ((float8 *)var->group_state)[1]) + ((float8 *)var->group_state)[0];
#else
  float8 cdf = (point < 0.5) ? (0.5 - point) : (point - 0.5);
  int index_low = 0, index_high = pip_normal_cdf_precomp_size-1, midpoint;

  if((point < 0.0) || (point > 1.0)) return NAN;
  
  //binary search
  if(pip_normal_cdf_precomp[pip_normal_cdf_precomp_size-1] > cdf){
    while(index_low < index_high){
      midpoint = (index_high + index_low) / 2;
      if(pip_normal_cdf_precomp[midpoint] < cdf){
        if(index_high == midpoint){
          index_high = midpoint -1;
        } else {
          index_high = midpoint;
        }
      } else {
        if(index_low == midpoint){
          index_low = midpoint +1;
        } else {
          index_low = midpoint;
        }
      }
    }
    if(pip_normal_cdf_precomp[index_low] == cdf){
      return cdf;
    }
    return 
      (((point < 0.5) ? (-1.0) : (1.0)) * 
      pip_normal_cdf_precision * 
      ((float8 *)var->group_state)[1] *
      ((float8)index_low + 
        (cdf - pip_normal_cdf_precomp[index_low]) / 
        (pip_normal_cdf_precomp[index_low+1] - pip_normal_cdf_precomp[index_low])))
      + ((float8 *)var->group_state)[0];
  } else {
    return 
      ((point < 0.5) ? (-1.0) : (1.0)) *
      ((float8 *)var->group_state)[1] *
      ( (0.5 - pip_normal_cdf_precomp[pip_normal_cdf_precomp_size-1]) *
        (pip_normal_cdf_precision * (double)pip_normal_cdf_precomp_size) /
        cdf)
      + ((float8 *)var->group_state)[0];
  }
#endif
}
int pip_normal_in   (pip_var *var, char *str)
{
  double mean,stddev;
  int c;
  
  sscanf(str, "%lf/%lf%n", &mean, &stddev, &c);
  
  ((float8 *)var->group_state)[0] = mean;
  ((float8 *)var->group_state)[1] = stddev;
  return c;
}
int pip_normal_out  (pip_var *var, int len, char *str)
{
  return snprintf(str, len, "%lf/%lf", (double)((float8 *)var->group_state)[0], (double)((float8 *)var->group_state)[1]);
}




