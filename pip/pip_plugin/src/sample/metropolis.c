//////////////////////////////////////////////////////////////////////////
// metropolis.c
// 
// An implementation of the metropolis random sampling algorithm
// 
//////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "postgres.h"
#include "pip.h"

typedef struct metropolis_var_state {
  pip_var *var;
  float8 stdDev;
  int sampleIndex;
} metropolis_var_state;

typedef struct metropolis_sampling_state {
  pip_sample_set *samples;
  pip_atom **clause;
  int clause_cnt;
  int64 seed;
  int sample;
  metropolis_var_state vars[0];
} metropolis_sampling_state;

bool metropolis_sample_step(int var, metropolis_sampling_state *state);

bool metropolis_sample_step(int var, metropolis_sampling_state *state)
{
  float8 step, dummy, old, P_curr, P_prime;
  int i;
  
  pip_box_muller(&step, &dummy, &state->seed);
  step *= state->vars[var].stdDev;
  
  old = pip_sample_val_get_by_id(
    state->samples, 
    state->vars[var].sampleIndex, 
    state->sample
  );
  
  P_curr = pip_var_pdf(state->vars[var].var, old);
  P_prime = pip_var_pdf(state->vars[var].var, old+step);
  
  if( (P_curr == 0.0) ||
      ( (P_prime < P_curr) && 
        (pip_prng_float(&state->seed) >= (P_prime/P_curr))
      )
    ){
    return false;
  }
  
  pip_sample_val_set_by_id(
    state->samples, 
    state->vars[var].sampleIndex,
    state->sample,
    old + step
  );
  
  for(i = 0; i < state->clause_cnt; i++){
    if(pip_atom_has_var(state->clause[i], state->vars[var].var)){
      if(!pip_atom_evaluate_sample(state->clause[i], state->samples, state->sample)){
        pip_sample_val_set_by_id(
          state->samples, 
          state->vars[var].sampleIndex,
          state->sample,
          old);
        return false;
      }
    }
  }
  
  return true;
}
