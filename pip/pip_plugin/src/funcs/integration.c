#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "eqn.h"

Datum   conf_one (PG_FUNCTION_ARGS)
{
  HeapTupleHeader   row = PG_GETARG_HEAPTUPLEHEADER(0);
  int32             samples = 1000;//(fcinfo->nargs > 1) ? (PG_GETARG_INT32(1)) : (1000);
  int               atom_count = 0;
  pip_atom        **atoms = NULL;
  float8            result;
  
  //SPI_connect();
  atom_count = pip_extract_clause(row, &atoms);
  result = pip_compute_independent_probability(atom_count, atoms, samples);
  //SPI_finish();
  
	PG_RETURN_FLOAT8(result);
}

Datum   pip_atom_conf_sample_g(PG_FUNCTION_ARGS)
{
  //since this is an aggregate, we don't need to detoast... I think.
  pip_conf_tally   *tally = (pip_conf_tally *)PG_GETARG_POINTER(0);
  HeapTupleHeader   row = PG_GETARG_HEAPTUPLEHEADER(1);
  pip_sample_set   *samples = (pip_sample_set *)PG_GETARG_BYTEA_P(2);
  int               atom_count = 0;
  pip_atom        **atoms = NULL;
  int i;
  
  SPI_connect();

  atom_count = pip_extract_clause(row, &atoms);

  for(i = 0; i < samples->sample_cnt; i++){
    if(pip_sample_test_clause(samples, i, atom_count, atoms)){
      if(!pip_conf_tally_up(tally, samples->ssid, i)){
        tally = pip_conf_tally_addgroup(tally, samples->ssid, samples->sample_cnt);
        pip_conf_tally_up(tally, samples->ssid, i);
      }
    }
  }
  
  SPI_finish();
  
  PG_RETURN_POINTER(tally);
}

Datum   pip_atom_sample_set_presence(PG_FUNCTION_ARGS)
{
  pip_world_presence     *wp = (pip_world_presence *)PG_GETARG_BYTEA_P(0);
  HeapTupleHeader         row = PG_GETARG_HEAPTUPLEHEADER(1);
  pip_sample_set         *set = (pip_sample_set *)PG_GETARG_BYTEA_P(2);
  pip_atom              **clause = NULL;
  int                     clause_cnt;
  int                     i, success = 0;
  
  clause_cnt = pip_extract_clause(row, &clause);

  if(wp->worldcount < set->sample_cnt){
    pip_world_presence *wp_old = wp;
    int len = set->sample_cnt;
    wp = palloc0(sizeof(pip_world_presence) + (len+7)/8);
    SET_VARSIZE(wp, sizeof(pip_world_presence) + (len+7)/8);
    wp->worldcount = len;
    //    elog(NOTICE, "Need to increase world size! %d->%d", wp_old->worldcount, wp->worldcount);
    memcpy(wp->data, wp_old->data, (wp_old->worldcount+7)/8);
  }
  
  for(i = 0; i < set->sample_cnt; i++){
    if(pip_sample_test_clause(set, i, clause_cnt, clause)) {
      continue;
    }
    success ++;
    wp->data[i/8] |= 1 << (7 - (i%8));
  }

  //elog(NOTICE, "finished clause scanning: (%d,%d) Result %d:%d / %d", wp->worldcount, set->sample_cnt, success, tot_success,set->sample_cnt * clause_cnt);
  
  PG_RETURN_POINTER(wp);
}
