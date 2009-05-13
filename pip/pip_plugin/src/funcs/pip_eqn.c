#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/geo_decls.h"

#include "pip.h"
#include "eqn.h"

Datum   pip_eqn_in (PG_FUNCTION_ARGS)
{
  char     *str = PG_GETARG_CSTRING(0);
  pip_eqn  *eqn;
  int       size = 0;
  
  pip_eqn_cmpnt_parse(str, NULL, 0, &size);
  size += sizeof(pip_eqn);
  
  eqn = SPI_palloc(size);
  bzero(eqn, size);
  SET_VARSIZE(eqn, size);
  
  pip_eqn_cmpnt_parse(str, eqn->data, 0, NULL);
  
  PG_RETURN_POINTER(eqn);
}


Datum   pip_eqn_out (PG_FUNCTION_ARGS)
{
  pip_eqn   *eqn = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  char      *str;
  
  str = palloc(500);
  pip_eqn_cmpnt_sprint(str, 500, eqn->data, 0);
  
  PG_RETURN_CSTRING(str);
}

Datum   pip_expectation (PG_FUNCTION_ARGS)
{
  pip_eqn        *eqn = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  HeapTupleHeader row = PG_GETARG_HEAPTUPLEHEADER(1);
  int32           samples = (fcinfo->nargs > 1) ? (PG_GETARG_INT32(2)) : (1000);
  float8          result;
  int             atom_count = 0;
  pip_atom      **atoms = NULL;
  
  SPI_connect();
  atom_count = pip_extract_clause(row, &atoms);
  result = pip_compute_expectation(eqn, atom_count, atoms, samples);
  SPI_finish();
  
  PG_RETURN_FLOAT8(result);
}

Datum   pip_expectation_max_g (PG_FUNCTION_ARGS)
{
  pip_sample_set     *set = (pip_sample_set *)PG_GETARG_BYTEA_P(0);
  pip_eqn            *eqn = (pip_eqn *)PG_GETARG_BYTEA_P(1); 
  HeapTupleHeader     row = (fcinfo->nargs > 2) ? (PG_GETARG_HEAPTUPLEHEADER(2)) : (NULL);
  int                 atom_count = 0;
  pip_atom          **atoms = NULL; 
  
  SPI_connect();
  if(row){
    atom_count = pip_extract_clause(row, &atoms);
  }
  set = pip_sample_set_vector_max(eqn, set, atom_count, atoms);
  SPI_finish();
  
  PG_RETURN_POINTER(set);
}

#define MAXPECTATION(set) (*ATOMSET_CACHE(set, float))

Datum   pip_expectation_max_g_one (PG_FUNCTION_ARGS)
{
  if(!fcinfo->context || !IsA(fcinfo->context, AggState)){
    elog(ERROR, "%s getting called, but not as an aggregate", __FUNCTION__);
  }
  if(((AggState *)fcinfo->context)->agg_done){
    PG_RETURN_POINTER(PG_GETARG_BYTEA_P(0));
  } else {
    pip_atomset    *state      = (pip_atomset *)PG_GETARG_BYTEA_P(0);
    float8          aggregate  = PG_GETARG_FLOAT8(1);
    HeapTupleHeader row        = PG_GETARG_HEAPTUPLEHEADER(2);
    int             atom_count = 0;
    pip_atom      **atoms      = NULL; 
    float8          prob;
    float8          oldexp;
    
//    if(state->count)
//      elog(ERROR, "----- %d", state->count);
    
    if(row){
      atom_count = pip_extract_clause(row, &atoms);
    }
    prob = pip_compute_conditioned_probability(state, atom_count, atoms, 1000);
    if(isnan(state->probability)){
      state->probability = 0;
    }
    state->probability += prob;
    MAXPECTATION(state) += prob * aggregate;
  
    if((1.0 - state->probability) * aggregate < MAXPECTATION(state) * 0.01){
      ((AggState *)fcinfo->context)->agg_done = true;
    }
    
//    elog(NOTICE, "Maximum at %lf, expectation: %lf, curr prob: %lf (%d negative constraints), cumulative probability: %lf, delta: %lf, done: %c", 
//      aggregate, 
//      MAXPECTATION(state), 
//      prob,
//      state->count,
//      state->probability, 
//      (1.0 - state->probability) * aggregate, 
//      ((AggState *)fcinfo->context)->agg_done ? 'y' : 'n'
//    );
    
    prob = state->probability;
    oldexp = MAXPECTATION(state);
    state = pip_atomset_by_appending(state, atom_count, atoms);
    MAXPECTATION(state) = oldexp;
    state->probability = prob;
    PG_RETURN_POINTER(state);
  }
}
Datum   pip_expectation_max_f_one (PG_FUNCTION_ARGS)
{
  pip_atomset    *state      = (pip_atomset *)PG_GETARG_BYTEA_P(0);
  
  PG_RETURN_FLOAT8(MAXPECTATION(state));
}

Datum   pip_expectation_sum_g (PG_FUNCTION_ARGS)
{
  pip_sample_set     *set = (pip_sample_set *)PG_GETARG_BYTEA_P(0);
  pip_eqn            *eqn = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  HeapTupleHeader     row = (fcinfo->nargs > 2) ? PG_GETARG_HEAPTUPLEHEADER(2) : (NULL);
  int                 atom_count = 0;
  pip_atom          **atoms = NULL;
  
  //log_eqn("Expect Sum", eqn->data);

  SPI_connect();
  if(row){
    atom_count = pip_extract_clause(row, &atoms);
  }
  set = pip_sample_set_vector_sum(eqn, set, atom_count, atoms);
  SPI_finish();
  
  PG_RETURN_POINTER(set);
}
Datum   pip_expectation_sum_g_one (PG_FUNCTION_ARGS)
{
  float8              val = PG_GETARG_FLOAT8(0);
  pip_eqn            *eqn = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  HeapTupleHeader     row = PG_GETARG_HEAPTUPLEHEADER(2);
  int                 samples = (fcinfo->nargs > 3) ? PG_GETARG_INT32(3) : (1000);
  int                 atom_count = 0;
  pip_atom          **atoms = NULL;
  
  SPI_connect();
  atom_count = pip_extract_clause(row, &atoms);
  val += pip_compute_expectation(eqn, atom_count, atoms, samples);
  SPI_finish();
  
  PG_RETURN_FLOAT8(val);
}

Datum   pip_eqn_sum_ee (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ee(PIP_EQN_ADD, X, Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sum_ei (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  int32     Y =            PG_GETARG_INT32  (1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, X, (double)Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sum_ie (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  int32     X =            PG_GETARG_INT32  (0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, Y, (double)X);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sum_ef (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  float8    Y =            PG_GETARG_FLOAT8 (1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, X, Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sum_fe (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  float8    X =            PG_GETARG_FLOAT8 (0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, Y, X);  
  
  PG_RETURN_POINTER(eqn);
}

Datum   pip_eqn_mul_ee (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ee(PIP_EQN_MULT, X, Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_mul_ei (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  int32     Y =            PG_GETARG_INT32  (1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_MULT, X, (double)Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_mul_ie (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  int32     X =            PG_GETARG_INT32  (0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_MULT, Y, (double)X);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_mul_ef (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  float8    Y =            PG_GETARG_FLOAT8 (1);

  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_MULT, X, Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_mul_fe (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  float8    X =            PG_GETARG_FLOAT8 (0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_MULT, Y, X);  
  
  PG_RETURN_POINTER(eqn);
}
Datum  pip_eqn_neg (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_one(PIP_EQN_NEGA, X);
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sub_ee (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ee(PIP_EQN_ADD, X, pip_eqn_compose_one(PIP_EQN_NEGA, Y));
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sub_ei (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  int32     Y =            PG_GETARG_INT32  (1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, X, (double)-Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sub_ie (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  int32     X =            PG_GETARG_INT32  (0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, pip_eqn_compose_one(PIP_EQN_NEGA, Y), (double)X);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sub_ef (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  pip_eqn  *X = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  float8    Y =            PG_GETARG_FLOAT8 (1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, X, -Y);  
  
  PG_RETURN_POINTER(eqn);
}
Datum   pip_eqn_sub_fe (PG_FUNCTION_ARGS)
{
  pip_eqn  *eqn;
  float8    X =            PG_GETARG_FLOAT8 (0);
  pip_eqn  *Y = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  
  elog(PIP_INTERNALS_LOGLEVEL, "%s", __FUNCTION__);
  eqn = pip_eqn_compose_ef(PIP_EQN_ADD, pip_eqn_compose_one(PIP_EQN_NEGA, Y), X);  
  
  PG_RETURN_POINTER(eqn);
}

Datum   pip_eqn_structural_equals (PG_FUNCTION_ARGS)
{
  pip_eqn *left  = (pip_eqn *)PG_GETARG_BYTEA_P(0), 
          *right = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  bool     result;
  
  result = pip_eqn_cmpnt_identical_structure(left->data, 0, right->data, 0);
  
  PG_RETURN_BOOL(result);
}

Datum   pip_eqn_subscript (PG_FUNCTION_ARGS)
{
  pip_eqn *eqn        = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  int64    subscript  = PG_GETARG_INT32(1);
  pip_eqn *sub_eqn;
    
  if(((pip_eqn_component *)eqn->data)->type != PIP_EQN_VAR){
    char buffer[400];
    pip_eqn_sprint(buffer, 400, eqn);
    elog(ERROR, "Invalid syntax '%s' not a variable", buffer);
  }
  
  if(PVAR_IS_JOINT(((pip_eqn_component *)eqn->data)->val.var.vid)){
    char buffer[400];
    pip_eqn_sprint(buffer, 400, eqn);
    elog(ERROR, "Invalid syntax '%s' not a joint distribution", buffer);
  }
  
  sub_eqn = pip_eqn_compose_sub(PIP_EQN_SUB, eqn, subscript);

  PG_RETURN_POINTER(sub_eqn);
}

Datum   pip_eqn_compile_sum (PG_FUNCTION_ARGS)
{
  pip_eqn            *left  = (pip_eqn *)PG_GETARG_BYTEA_P(0);
  pip_eqn            *right = (pip_eqn *)PG_GETARG_BYTEA_P(1);
  HeapTupleHeader     row;
  pip_atom          **atoms = NULL;
  int                 atom_count = 0;
  int                 size = VARSIZE(right) - sizeof(pip_eqn);
  int                 i;
  
  SPI_connect();
  if(fcinfo->nargs > 2){
    row = PG_GETARG_HEAPTUPLEHEADER(2);
    atom_count = pip_extract_clause(row, &atoms);
    for(i = 0; i < atom_count; i++){
      size += sizeof(pip_eqn_component) + VARSIZE(atoms[i]);
    }
  }
  
  i = 0;
  left = pip_eqn_append_slot_ee(PIP_EQN_ADD, left, size, &i);
  if(fcinfo->nargs > 2){
    size = pip_eqn_fill_in_constraints(left->data, i, atoms, atom_count);
  } else {
    size = i;
  }
  
  memcpy(DEREF_CMPNT(left->data, size), right->data, VARSIZE(right) - sizeof(pip_eqn));
  pip_eqn_cmpnt_update_pointers(left->data+size, 0, size);

  SPI_finish();
  
  PG_RETURN_POINTER(left);
}
