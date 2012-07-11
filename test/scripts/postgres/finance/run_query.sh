#!/bin/bash

if [ $# -eq 0 ]
then
  echo "Usage: `basename $0` [all|axfinder|...] [all|standard|tiny|big|huge]"
  exit 1
fi

SCRIPT_DIR=`dirname $0`
COMMAND=$0
QUERY=$1
DATASET=$2

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

    for DATASET in tiny standard big huge
    do
      $COMMAND $QUERY $DATASET $DELIMITER
    done

  else

    OUTPUT_FILE=${QUERY}_${DATASET}.csv
    echo "Running $QUERY on $DATASET"

    sed -e "s:@@DATASET@@:$DATASET:g" $SCRIPT_DIR/queries/$QUERY.sql > _query.sql
    psql -AqtF "," -f _query.sql -o $OUTPUT_FILE
    rm -f _query.sql

    echo "Done. The result is in $OUTPUT_FILE."
  fi
fi
