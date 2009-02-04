#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"

#include "pip.h"
#include "value_bundle.h"

Datum   pip_world_presence_in   (PG_FUNCTION_ARGS)
{
  char               *str = PG_GETARG_CSTRING(0);
  int32               len;
  pip_world_presence *wp;
  int                 c;

  if((str[0] == '?') || (str[0] == '!')){
    if(sscanf(str+1, "%d", &len) != 1){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("World Presence, couldn't load length (fresh)!")));
    }
    wp = palloc0(sizeof(pip_world_presence) + (len+7)/8);
    SET_VARSIZE(wp, sizeof(pip_world_presence) + (len+7)/8);
    wp->worldcount = len;
    memset(wp->data, (str[0] == '!') ? 0xff : 0x00, (len+7)/8);
    
  } else {
    if(sscanf(str, "%d:%n", &len, &c) != 2){
      ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("World Presence, couldn't load length! (reload)")));
    }
    str += c;
    
    elog(PIP_MCDB_INFO_LOGLEVEL, "Allocating world presence (in) : %d bytes, %d length", (int)(sizeof(pip_world_presence) + (len+7)/8), len);
    wp = palloc0(sizeof(pip_world_presence) + (len+7)/8);
    SET_VARSIZE(wp, sizeof(pip_world_presence) + (len+7)/8);
    wp->worldcount = len;
    
    for(c = 0; c < len; c+=4){ //read in one nibble at a time
      wp->data[c/2] <<= 4;
      wp->data[c/2] |= 
        (unsigned char)
        (((str[c/4] >= '0')&&(str[c/4] <= '9')) ? (str[c/4] - '0') : 
          (((str[c/4] >= 'a')&&(str[c/4] <= 'f')) ? (str[c/4] - 'a' + 10) : 
            (((str[c/4] >= 'A')&&(str[c/4] <= 'F')) ? (str[c/4] - 'a' + 10) : 0)
          )
        );
    }
  }
  
  PG_RETURN_POINTER(wp);
}
Datum   pip_world_presence_out  (PG_FUNCTION_ARGS)
{
  pip_world_presence *wp = (pip_world_presence *)PG_GETARG_BYTEA_P(0);
  char               *result;
  int                 c = 0, i;
  unsigned char       curr;
  
  result = (char *) palloc0(100+wp->worldcount/4);
  
  c += snprintf(result, 400, "%d:", wp->worldcount);
  for(i = 0; i*4 < wp->worldcount; i++){ //write out one nibble at a time
    curr = (wp->data[i/2] >> (4 * ((i+1)%2))) & 0xf;
    result[c] = (curr >= 10) ? ('a'+(curr-10)) : ('0'+curr);
    c++;
  }
  result[c] = '\0';

  PG_RETURN_CSTRING(result);
}
Datum   pip_world_presence_create(PG_FUNCTION_ARGS)
{
  int64               len = PG_GETARG_INT32(0);
  pip_world_presence *wp;
  
  elog(PIP_MCDB_INFO_LOGLEVEL, "Allocating world presence (create) : %d bytes, %d length", (int)(sizeof(pip_world_presence) + (len+7)/8), (int)len);
  wp = palloc0(sizeof(pip_world_presence) + (len+7)/8);
  SET_VARSIZE(wp, sizeof(pip_world_presence) + (len+7)/8);
  wp->worldcount = len;
  memset(wp->data, 0xff, (len+7)/8);
  PG_RETURN_POINTER(wp);
}
Datum   pip_world_presence_count(PG_FUNCTION_ARGS)
{
  pip_world_presence *wp = (pip_world_presence *)PG_GETARG_BYTEA_P(0);
  int                 i;
  int                 cnt = 0;
  float8              result;
  
  for(i = 0; i < wp->worldcount; i++){
    cnt += ((wp->data[i/8] >> (7-(i%8)))&0x01);
  }
  
  result = ((float8)cnt) / ((float8)wp->worldcount);
  
  PG_RETURN_FLOAT8(result);
}
Datum   pip_world_presence_union (PG_FUNCTION_ARGS)
{
  pip_world_presence *wp1 = (pip_world_presence *)PG_GETARG_BYTEA_P_COPY(0);
  pip_world_presence *wp2 = (pip_world_presence *)PG_GETARG_BYTEA_P(1);
  int                 i;
  
  //  elog(NOTICE, "UNION: %d(%d),%d(%d)", wp1->worldcount, VARSIZE(wp1), wp2->worldcount, VARSIZE(wp2));
  for(i = 0; i < (MIN(wp1->worldcount, wp2->worldcount)+7) / 8; i++){
    wp1->data[i] |= wp2->data[i];
  }
  //  elog(NOTICE, "UNION COMPLETE");
  PG_RETURN_POINTER(wp1);
}