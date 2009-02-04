#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "conf_tally.h"

Datum pip_conf_tally_in(PG_FUNCTION_ARGS)
{
  char             *str = PG_GETARG_CSTRING(0);
  pip_conf_tally   *tally;
  long              sample_count, group_count, ssid, group_samples, sample_fill = 0;
  int               c, i;
  
  i = sscanf(str, "%ld:%ld%n", &sample_count, &group_count, &c);
  if((i != 3)&&((i != 2)||((sample_count != 0)&&(sample_count != 0)))){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("text representation WRONG! (ctin1:%d,%ld,%ld):'%s'", i, sample_count, group_count,str)));
  }
  if(i == 2){
    c = strlen(str);
  }
  str+=c;
    
  tally = pip_conf_tally_create(sample_count, group_count);
  for(i = 0; i < group_count; i++){
    if(sscanf(str, "|%ld:%ld%n", &ssid, &group_samples, &c) != 3){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("text representation WRONG!(ctin2:%d)",i)));
    }
    str+=c;
    TALLY_GROUP[i].ssid = ssid;
    TALLY_GROUP[i].sample_cnt = group_samples;
    sample_fill += group_samples;
  }
  for(i = 0; i < sample_fill; i++){
    if(sscanf(str, "{%ld}%n", &ssid, &c) != 3){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("text representation WRONG!(ctin3:%d)",i)));
    }
    str+=c;
    TALLY_COUNT[i] = ssid;
  }
  
  PG_RETURN_POINTER(tally);
}
Datum pip_conf_tally_out(PG_FUNCTION_ARGS)
{
  pip_conf_tally *tally = (pip_conf_tally *)PG_GETARG_BYTEA_P(0);
  char           *output = palloc(550);
  int             c = 0, i;
  long            sample_fill = 0;
  
  c = snprintf(output, 550, "%ld:%ld", (long)tally->sample_cnt, (long)tally->group_cnt);
  for(i = 0; i < tally->group_cnt; i++){
    c += snprintf(output+c, 550-c, "|%ld:%ld", (long)TALLY_GROUP[i].ssid, (long)TALLY_GROUP[i].sample_cnt);
    sample_fill += TALLY_GROUP[i].sample_cnt;
  }
  for(i = 0; i < sample_fill; i++){
    c += snprintf(output+c, 550-c, "{%ld}", (long)TALLY_COUNT[i]);
  }
  PG_RETURN_CSTRING(output);
}

Datum   pip_conf_tally_result(PG_FUNCTION_ARGS)
{
  pip_conf_tally   *tally = (pip_conf_tally *)PG_GETARG_POINTER(0);
//  elog(PIP_CONF_GATHER_LOGLEVEL, "Gathering Final: Done");
  PG_RETURN_FLOAT8(pip_conf_tally_compute_result(tally));
}

