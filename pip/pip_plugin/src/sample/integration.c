//////////////////////////////////////////////////////////////////////////
// integration.c                                                        
// 
// Operations to compute probability densities contained within regions
//    defined by zero or more constraint atoms, as well as expectations
//    of variables (given zero or more constraint atoms)
// 
//////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "postgres.h"
#include "pip.h"
#include "atomset.h"

typedef struct pip_prob_comp_info {
  int             clause_cnt;
  int             clause_sorted;
  pip_atom      **clause;
  float8          result;
  int64           samples;
  pip_atomset    *constraints;
} pip_prob_comp_info;

static float8 pip_integrate_by_sampling(pip_atom **atoms, int first_atom, int num_atoms, int64 num_samples, pip_atomset *constraints);
static int pip_compute_prob_group(pip_cset *variables, pip_cset_element *group, pip_prob_comp_info *info);
static float8 pip_integrate_by_cdf(pip_atom **atoms, int first_atom, int num_atoms, pip_var *var);

static float8 pip_integrate_by_cdf(pip_atom **atoms, int first_atom, int num_atoms, pip_var *var)
{
  float8 bounds[2];
  if(pip_solve_cdf_bounds(&atoms[first_atom], num_atoms, var, bounds, true)){
    return (bounds[1] > bounds[0]) ? (bounds[1] - bounds[0]) : (0.0);
  } else {
    return NAN;
  }
}

static float8 pip_integrate_by_sampling(pip_atom **atoms, int first_atom, int num_atoms, int64 num_samples, pip_atomset *constraints)
{
  int i, sample;
  int64 seed, count = 0;
  int64 satfailcount = 0;
  for(sample = 0; sample < num_samples; sample++){
    seed = random();
    count++;
    for(i = 0; i < num_atoms; i++){
      if(!pip_atom_evaluate_seed(atoms[i+first_atom], seed)){
        count--;
        break;
      }
    }
    if(i >= num_atoms){
      if((constraints->count > 0) && !pip_atomset_unsatisfied_seed(constraints, seed)){
        count--; 
        satfailcount++;
      }
    }
  }
  return ((float8)count) / ((float8)num_samples);
}

static int pip_compute_prob_group(pip_cset *variables, pip_cset_element *group, pip_prob_comp_info *info)
{
  int first_atom;
  first_atom = info->clause_sorted;
  
  elog(PIP_INTEGRATE_LOGLEVEL, "Sorting out included atoms");
  info->clause_sorted = pip_group_atoms(variables, group, info->clause_cnt, info->clause, info->clause_sorted);
  
  //This would be where we pick the ideal integration scheme for this variable group.  
  //for now, all we have is a sampling mechanism.
  switch(pip_cset_group_size(variables, group)){
    case 0: 
       //nothing in this clause?  Ooopsie.
      elog(ERROR, "Serious error in PIP; Empty Probability Group : %s:%d", __FILE__, __LINE__);
      break;
    case 1:
       //One variable in the clause lets us use CDFs to integrate
      //assuming we have them.
      if(!info->constraints){ //inverted constraints kinda screw with cdf sampling... lay off for now
        if(PVAR_HAS_CDF(((pip_var *)group->item)->vid)){
          float8 localResult;
           localResult = pip_integrate_by_cdf(info->clause, first_atom, info->clause_sorted - first_atom, ((pip_var *)group->item));
           //still some formulae for which it isn't possible to do this sort of integration.
          if(!isnan(localResult)){
            info->result *= localResult;
            break;
          }
        }
      }
       //if we don't, fall through to standard sampling.
    default:
       //If we've got more than one variable in the clause, we need to resort to sampling.
      info->result *= pip_integrate_by_sampling(info->clause, first_atom, info->clause_sorted - first_atom, info->samples, info->constraints);
      break;
  }
  
  //other mechanisms might include a numerical integrator for 1 variable
  //or possibly a numerical integrator that takes advantage of the CDF if possible for 2 variables.
  
  return 0;
}

float8 pip_compute_conditioned_probability(pip_atomset *constraints, int clause_cnt, pip_atom **clause, int samples)
{
  pip_cset vars;
  pip_prob_comp_info info;
  
  elog(PIP_INTEGRATE_LOGLEVEL, "Generating CSET from atoms: %d", clause_cnt);
  pip_clause_to_cset(clause_cnt, clause, &vars);
  
  info.clause_cnt = clause_cnt;
  info.clause = clause;
  info.samples = samples;
  info.result = 1.0;
  info.clause_sorted = 0;
  info.constraints = constraints;
  
  elog(PIP_INTEGRATE_LOGLEVEL, "Computing group probabilities");
  pip_cset_iterate_roots(&vars, (pip_cset_iterator *)&pip_compute_prob_group, &info);
  
  elog(PIP_INTEGRATE_LOGLEVEL, "Cleaning up CSET (result: %lf)", info.result);
  pip_cset_cleanup(&vars);
  
  return info.result;
}

float8 pip_compute_independent_probability(int clause_cnt, pip_atom **clause, int samples)
{
  return pip_compute_conditioned_probability(NULL, clause_cnt, clause, samples);
}
float8 pip_compute_expectation(pip_eqn *eqn, int clause_cnt, pip_atom **clause, int64 samples)
{
  float8 val = 0.0;
  float8 probability;
  int64 i;
  pip_sample_set *set;
  
  //don't go through the overhead of sampling if we can avoid it.
  if(clause_cnt <= 0) return pip_compute_expectation_conditionless(eqn, samples);
  
  set = pip_sample_by_clause(clause_cnt, clause, samples, &probability);
  for(i = 0; i <  samples; i++){
    val += pip_eqn_evaluate_sample(eqn, set, i);
  }
//  elog(NOTICE, "probability: %lf, %lf = %lf/%ld samples", (float)probability,  (val / (float8)samples) * probability, val, samples);
  return (val / (float8)samples) * probability;
}

float8 pip_compute_expectation_conditionless(pip_eqn *eqn, int64 samples)
{
  float8 val = 0.0;
  int64 i;
  for(i = 0; i <  samples; i++){
    val += pip_eqn_evaluate_seed(eqn, random());
  }
  return val / (float8)samples;
}
