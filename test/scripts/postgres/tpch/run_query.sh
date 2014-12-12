#!/bin/bash

if [ $# -lt 2 ]
then
  echo "Usage: `basename $0` (all|query3|query11|...) (all|standard|big|tiny|standard_del|...) [delimiter='|']"
  exit 1
fi

SCRIPT_DIR=`dirname $0`
COMMAND=$0
QUERY=$1
DATASET=$2
OUTPUT_FILE=${QUERY}_${DATASET}.csv
DELIMITER="|"

if [ $# -gt 2 ]
then
  DELIMITER=$3
fi

if [ $QUERY = "all" ]
then
 
  QUERIES=`ls queries/*.sql`
  for QUERY_EXT in `ls queries/*.sql` 
  do
    QUERY=`basename $QUERY_EXT .sql`
    $COMMAND $QUERY $DATASET $DELIMITER
  done

else 
  if [ $DATASET = "all" ]
  then

    for DATASET in tiny tiny_del standard standard_del big big_del 
    do
      $COMMAND $QUERY $DATASET $DELIMITER
    done

  else

    echo "Running $QUERY on $DATASET"

    sed -e "s:@@DATASET@@:$DATASET:g" $SCRIPT_DIR/queries/$QUERY.sql > _query.sql
    psql -d dbtoaster -AqtF $DELIMITER -f _query.sql -o $OUTPUT_FILE
    rm -f _query.sql

    echo "Done. The result is in $OUTPUT_FILE."

  fi
fi
