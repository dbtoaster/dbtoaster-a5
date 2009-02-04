#!/bin/bash

# Error on functions in the SQL command that haven't been declared in the C code
( grep "CREATE FUNCTION" `dirname $0`/../src/pip.source | 
      grep "LANGUAGE C" | 
      sed 's/^[^)]*)[^,]*,'"'"'\([^ ]*\)'"'"'.*/\1/' | 
      sort | uniq; 
  grep -rh PG_FUNCTION_ARGS `dirname $0`/../src/funcs | 
      sed 's/Datum *\([^ (]*\).*/\1/' | 
      sort | uniq; 
  grep -rh PG_FUNCTION_ARGS `dirname $0`/../src/funcs | 
      sed 's/Datum *\([^ (]*\).*/\1/' | 
      sort | uniq;
) | sort | uniq -u > error_funcs.tmp

if [ -s "error_funcs.tmp" ] ; then
  echo "------- Unimplemented SQL Functions -------";
  cat error_funcs.tmp;
  rm error_funcs.tmp
  exit -1;
fi
rm error_funcs.tmp

# generate funcs.c and funcs.h
grep -rh PG_FUNCTION_ARGS `dirname $0`/../src/funcs | 
    awk '
      BEGIN { print "#ifndef PIP_FUNCS_H_SHIELD"; 
              print "#define PIP_FUNCS_H_SHIELD"; } 
            { print $0,";"; }
      END   { print "#endif"; }
    ' > `dirname $0`/../src/include/funcs.h

echo '#include <stdio.h>'             >  `dirname $0`/../src/funcs.c
echo '#include <stdlib.h>'            >> `dirname $0`/../src/funcs.c
echo '#include <string.h>'            >> `dirname $0`/../src/funcs.c
echo '#include "postgres.h"'          >> `dirname $0`/../src/funcs.c
echo '#include "fmgr.h"'              >> `dirname $0`/../src/funcs.c
echo '#include "funcapi.h"'           >> `dirname $0`/../src/funcs.c
echo '#include "executor/executor.h"' >> `dirname $0`/../src/funcs.c
echo '#include "executor/spi.h"'      >> `dirname $0`/../src/funcs.c
echo '#include "pip.h"'               >> `dirname $0`/../src/funcs.c
echo '#include "funcs.h"'             >> `dirname $0`/../src/funcs.c


grep -rh PG_FUNCTION_ARGS `dirname $0`/../src/funcs/*.c | 
    sed 's/Datum *\([^ (]*\).*/PG_FUNCTION_INFO_V1(\1);/' >> `dirname $0`/../src/funcs.c
for i in $(ls `dirname $0`/../src/funcs/*.c); do
  echo `basename $i` | 
      sed 's/^/#include "funcs\//;s/$/"/;' >> `dirname $0`/../src/funcs.c
done
exit 0;