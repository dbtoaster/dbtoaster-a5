#include <stdio.h>
#include <stdlib.h>

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/typcache.h"
#include "utils/memutils.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"

#include "pip.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*************  INTERNAL MANAGEMENT  ***************/
void _PG_init(void)
{
  elog(NOTICE, "=== PIP Probabilistic Database System ===");
}

/*************  DISTRIBUTION PARAMETER SUPPORT  ***************/
float8 dist_param_float8(HeapTupleHeader params, int paramnum, float8 default_value)
{
	Oid			          tupType;
	int32		          tupTypmod;
	TupleDesc	        tupDesc;
	HeapTupleData     tmptup;
	Datum             param_datum;
  bool              isnull;
  double            retval = default_value;

	tupType = HeapTupleHeaderGetTypeId(params);
	tupTypmod = HeapTupleHeaderGetTypMod(params);
	tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	
	if(tupDesc->natts >= paramnum){ 
    tmptup.t_len = HeapTupleHeaderGetDatumLength(params);
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = params;
    
    param_datum = heap_getattr(&tmptup,
                        tupDesc->attrs[paramnum]->attnum,
                        tupDesc,
                        &isnull);
    if(!isnull){
      switch(tupDesc->attrs[paramnum]->atttypid){
        case BOOLOID:
          retval = DatumGetBool(param_datum) ? 1.0 : 0.0;
          break;
        case INT8OID:
          retval = (float8)DatumGetInt64(param_datum);
          break;
        case INT4OID:
          retval = (float8)DatumGetInt32(param_datum);
          break;
        case INT2OID:
          retval = (float8)DatumGetInt16(param_datum);
          break;
        case FLOAT4OID:
          retval = (float8)DatumGetFloat4(param_datum);
          break;
        case FLOAT8OID:
          retval = (float8)DatumGetFloat8(param_datum);
          break;
        case NUMERICOID:
        { // backend/utils/adt/numeric.c: numeric_to_double_no_overflow()
          char	   *tmp;
          double		val;
          char	   *endptr;
        
          tmp = DatumGetCString(DirectFunctionCall1(numeric_out,param_datum));
        
          /* unlike float8in, we ignore ERANGE from strtod */
          val = strtod(tmp, &endptr);
          if (*endptr != '\0')
          {
            /* shouldn't happen ... */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
               errmsg("invalid input syntax for type double precision: \"%s\"",
                  tmp)));
          }
          pfree(tmp);
          retval = (float8)val;
          break;
        }
        default:
          ereport(ERROR,
              (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("Distribution takes double precision parameter and PIP can not cast type: %s",
                format_type_be(tupDesc->attrs[paramnum]->atttypid))));
      }
    }
  }
	ReleaseTupleDesc(tupDesc);
	
	return retval;
}

/*************  VARGEN SUPPORT  ***************/
//Numerical Recipes v3: Sec 7.14
int64 pip_prng_step(int64 v)
{

  v = v * 3935559000370003845LL + 2691343689449507681LL;
  v ^= v >> 21; v^= v << 37; v ^= v >> 4;
  v *= 4768777413237032717LL;
  v ^= v >> 20; v^= v << 41; v ^= v >> 5;

  return v;
}

int64 pip_prng_int(int64 *seed)
{
  *seed = pip_prng_step(*seed);
  return *seed;
}

float8 pip_prng_float(int64 *seed)
{
  return (1.0 / ((float8)RANDOM_MAX)) * (float8)(pip_prng_int(seed) & RANDOM_MAX);
}

