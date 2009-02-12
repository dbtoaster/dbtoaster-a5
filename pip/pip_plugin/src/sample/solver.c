//////////////////////////////////////////////////////////////////////////
// solver.c
// 
// Operations used to manipulate pip_atoms semantically, generally to
// extract information; eg: upper bound/lower bound, or equivalence
// 
//////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "postgres.h"
#include "pip.h"

float8 pip_solve_bound(pip_atom *clause, pip_var *var, bool *isupper)
{
  float8 left_mult = 0.0, left_const = 0.0, right_mult = 0.0, right_const = 0.0;
  
  if(pip_eqn_cmpnt_simplify_1var(clause->data, clause->ptr_left, &left_const, &left_mult, var) &&
     pip_eqn_cmpnt_simplify_1var(clause->data, clause->ptr_right, &right_const, &right_mult, var)){
    //elog(NOTICE, "simplify: %lfX+%lf > %lfX+%lf", left_mult, left_const, right_mult, right_const);
    left_mult -= right_mult;
    if(left_mult == 0.0) return NAN;
    right_const -= left_const;
    right_const /= left_mult;
    *isupper = left_mult < 0;
    return right_const;
  }
  return NAN;
}

bool pip_solve_cdf_bounds(pip_atom **atoms, int num_atoms, pip_var *var, float8 bounds[2], bool tight)
{
  int i;
  float8 val;
  bool isupper, have_lower = false, have_upper = false;
  
  if(!PVAR_HAS_CDF(var->vid)) return false;
  
  bounds[0] = -INFINITY;
  bounds[1] = INFINITY;
  
  for(i = 0; i < num_atoms; i++){
    val = pip_solve_bound(atoms[i], var, &isupper);
    if(isnan(val)){ 
      if(tight) return false; 
    } else {
      if(isupper){
        if(bounds[1] > val){ bounds[1] = val; have_upper = true; }
      } else {
        if(bounds[0] < val){ bounds[0] = val; have_lower = true; }
      }
    }
  }
  if(have_lower){ bounds[0] = pip_var_cdf(var, bounds[0]); } 
  else { bounds[0] = 0.0; }
  if(have_upper){ bounds[1] = pip_var_cdf(var, bounds[1]); } 
  else { bounds[1] = 1.0; }
  return true;
}
