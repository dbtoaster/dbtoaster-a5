#!/bin/bash

if [ $# -eq 0 ]
then
  echo "Usage: `basename $0` [query3|query11|...] [standard|tiny|big|huge]"
  exit 1
fi

SCRIPT_DIR=`dirname $0`
QUERY=$1
DATASET=$2
OUTPUT_FILE=${QUERY}_${DATASET}.csv

echo "Running $QUERY on $DATASET"

sed -e "s:@@DATASET@@:$DATASET:g" $SCRIPT_DIR/queries/$QUERY.sql > _query.sql
psql -AqtF "," -f _query.sql -o $OUTPUT_FILE
rm -f _query.sql

echo "Done. The result is in $OUTPUT_FILE."
