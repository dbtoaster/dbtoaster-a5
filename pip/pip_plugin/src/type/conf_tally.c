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
#include "conf_tally.h"

pip_conf_tally *pip_conf_tally_create(int32 sample_count, int group_count)
{
  size_t varsize  = sizeof(pip_conf_tally) +
                    sizeof(pip_conf_tally) * group_count +
                    sizeof(int32) * sample_count;
  pip_conf_tally *ret = SPI_palloc(varsize);
  bzero(ret, varsize);
  SET_VARSIZE(ret, varsize);
  
  ret->group_cnt = group_count;
  ret->sample_cnt = sample_count;

  return ret;
}

pip_conf_tally *pip_conf_tally_addgroup(pip_conf_tally *old, int64 ssid, int group_count)
{
  pip_conf_tally *tally = pip_conf_tally_create(old->sample_cnt + group_count, old->group_cnt + 1);

  TALLY_GROUP[old->group_cnt].ssid = ssid;
  TALLY_GROUP[old->group_cnt].sample_cnt = group_count;
  
  memcpy(tally->data, old->data, sizeof(pip_conf_tally_group) * old->group_cnt);
  memcpy( (tally->data + sizeof(pip_conf_tally_group) * tally->group_cnt), 
          (old->data + sizeof(pip_conf_tally_group) * old->group_cnt),
          sizeof(int32) * old->sample_cnt);
  //pfree(old);
  
  return tally;
}

bool pip_conf_tally_up(pip_conf_tally *tally, int64 ssid, int sample_id)
{
  int i, curr_id = sample_id;
  for(i = 0; i < tally->group_cnt; i++){
    if(TALLY_GROUP[i].ssid == ssid){
      break;
    }
    curr_id += TALLY_GROUP[i].sample_cnt;
  }
  if(i >= tally->group_cnt){
    return false;
  }
  TALLY_COUNT[curr_id]++;
  elog(PIP_SAMPLE_LOGLEVEL, "Updated Tally Count for Possible World: %d is now %d (SSID %d: Group %d)", curr_id, (int)TALLY_COUNT[curr_id], (int)TALLY_GROUP[i].ssid, i);
  return true;
}

float8 pip_conf_tally_compute_result(pip_conf_tally *tally)
{
  float8 accum = 0.0;
  int i, size_accum = 0, group = 0;
  for(i = 0; i < tally->sample_cnt; i++){
    if(i >= size_accum + TALLY_GROUP[group].sample_cnt){
      size_accum += TALLY_GROUP[group].sample_cnt;
      group++;
    }
    if(TALLY_COUNT[i] == 0){
      elog(ERROR, "Tally Count for Possible World: %d is zero! (SSID %d: Group %d)", i, (int)TALLY_GROUP[group].ssid, i-(int)size_accum);
      //tally count is never zero..
      //return NAN;
      continue;
      accum += 1.0;
    }
    accum += 1.0 / ((float8)TALLY_COUNT[i]);
  }
  return accum / ((float8)tally->sample_cnt);
}
