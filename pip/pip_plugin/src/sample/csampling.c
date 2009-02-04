//////////////////////////////////////////////////////////////////////////
// csampling.c                                                        
// 
// Operations to generate samples given a set of constraints (ie, those
//   specified by a set of constraint atoms.)
// 
//////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "postgres.h"
#include "pip.h"
#include "dist.h"

typedef struct pip_sampler_state {
  pip_sample_set *samples;
  int curr_sample;
  int clause_cnt;
  pip_atom **clause;
  int first_atom, last_atom;
  int first_var, last_var;
  float8 probability;
} pip_sampler_state;

static void rejection_sample(pip_cset *set, pip_cset_element *group, pip_sampler_state *state);
static int sample_one(pip_cset *set, pip_var *var, pip_sampler_state *state);
static int populate_sample_vars(pip_cset *set, pip_var *item, pip_sampler_state *state);
static int sample_lineage_group(pip_cset *set, pip_cset_element *group, pip_sampler_state *state);

/*********************** Sampling Techniques *********************/
static int sample_one(pip_cset *set, pip_var *var, pip_sampler_state *state)
{
  float8 val = pip_var_gen(var);
//  //the iteration order is the same as the one used to populate the variables.
//  pip_sample_val_set_by_id(state->samples, state->last_var, state->curr_sample, val);
  pip_sample_val_set(state->samples, &var->vid, state->curr_sample, val);
  state->last_var++;
  return 0;
}

static void rejection_sample(pip_cset *set, pip_cset_element *group, pip_sampler_state *state)
{
  int cnt = 0;
  for(; state->curr_sample < state->samples->sample_cnt; state->curr_sample++){
    do {
      state->last_var = state->first_var;
      pip_cset_iterate_group(set, group, (pip_cset_iterator *)&sample_one, state);
      cnt++;
    } while(
      !pip_sample_test_clause(
          state->samples, 
          state->curr_sample, 
          state->last_atom-state->first_atom, 
          &state->clause[state->first_atom]
        )
      );
  }
  if(state->samples->sample_cnt > 0)
    state->probability *= (float8)state->samples->sample_cnt / (float8)cnt;
}

static bool cdf_sample_var(pip_cset *set, pip_var *var, pip_sampler_state *state){
  int cnt = 0;
  float8 bounds[2], sample;
  int first_sample = state->curr_sample;

  //even if we can't get tight bounds, we can still benefit from a narrower selection range.
  //the only requirement is that we have both a CDF and an ICDF.
  
  if(!PVAR_HAS_2WAY_CDF(var->vid)) return false;
  
  if(!pip_solve_cdf_bounds(&state->clause[state->first_atom], (state->last_atom - state->first_atom), var, bounds, false)){
    return false;
  }
  
  for(; state->curr_sample < state->samples->sample_cnt; state->curr_sample++){
    do {
      sample = pip_var_gen_w_range(var, bounds[0], bounds[1]);
      if(isnan(sample)) {
        state->curr_sample = first_sample;
        return false;
      }
      pip_sample_val_set(state->samples, &var->vid, state->curr_sample, sample);
      cnt++;
    } while(pip_sample_test_clause(
          state->samples, 
          state->curr_sample, 
          state->last_atom-state->first_atom, 
          &state->clause[state->first_atom]
        )
      );
  }
  state->last_var++;
  return true;
}

/*********************** Internal Functions **********************/

static int populate_sample_vars(pip_cset *set, pip_var *item, pip_sampler_state *state)
{
  pip_sample_name_set(state->samples, state->last_var, &item->vid);
  state->last_var++;
  return 0;
}

static int sample_lineage_group(pip_cset *set, pip_cset_element *group, pip_sampler_state *state)
{
  state->first_atom = state->last_atom;
  state->last_atom = pip_group_atoms(set, group, state->clause_cnt, state->clause, state->first_atom);
  state->first_var = state->last_var;
  
  state->curr_sample = 0;
  
  pip_cset_iterate_group(set, group, (pip_cset_iterator *)&populate_sample_vars, state);

  //this is where we figure out the best way to sample. 
  //for now, all we do is rejection sampling
  switch(pip_cset_group_size(set, group)){
    case 1:
      if(cdf_sample_var(set, ((pip_var *)group->item), state)) break;
      //if we're unable to employ the cdf (no cdf available, or eqn too complex), fall through to rejection
    default:
      rejection_sample(set, group, state);
      break;
  }
  
  return 0;
}

pip_sample_set *pip_sample_by_clause   (int clause_cnt, pip_atom **clause, int sample_cnt, float8 *probability)
{
  pip_sampler_state     state;
  pip_cset              varset;
  
  if(sample_cnt <= 0){
    return NULL;
  }
  
  pip_clause_to_cset(clause_cnt, clause, &varset);
  //As of this moment, it is critical that the group iteration order be preserved
  //and any group membership test on an element of varset could potentially
  //change that.  Locking the cset will enforce this invariant by causing all 
  //subsequent link operations to fail, and disabling path compression on group 
  //membership tests.
  //As of 11/21/08, this does not affect performance at all.
  pip_cset_lock(&varset);
  
  state.samples    = pip_sample_set_create(sample_cnt, pip_cset_size(&varset));
  state.first_atom = 0;
  state.last_atom  = 0;
  state.first_var  = 0;
  state.last_var   = 0;
  state.clause_cnt = clause_cnt;
  state.clause     = clause;
  state.probability = 1.0;
  
  pip_cset_iterate_roots(&varset, (pip_cset_iterator *)&sample_lineage_group, &state);
  
  if(probability) { *probability = state.probability; }
  
  return state.samples;
}

bool pip_sample_test_clause(pip_sample_set *set, int sample, int clause_cnt, pip_atom **clause)
{
  int i;
  for(i = 0; i < clause_cnt; i++){
    if(!pip_atom_evaluate_sample(clause[i], set, sample)) { 
      return false;
    }
  }
  return true;
}
