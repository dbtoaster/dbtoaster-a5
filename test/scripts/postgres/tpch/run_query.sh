#!/bin/bash

if [ $# -eq 0 ]
then
  echo "Usage: `basename $0` [query3|query11|...] [standard|big|tiny|standard_del|...]"
  exit 1
fi

SCRIPT_DIR=`dirname $0`
QUERY=$1
DATASET="TPCH_standard"
OUTPUT_FILE=$QUERY

case $2 in
   standard)     echo "STD"
                 DATASET="TPCH_standard"
                 OUTPUT_FILE=${QUERY}.csv
                 ;;
   standard_del) DATASET="TPCH_standard_del"
                 OUTPUT_FILE=${QUERY}_del.csv 
                 ;;
   big)          DATASET="TPCH_big" 
                 OUTPUT_FILE=${QUERY}_big.csv 
                 ;;
   big_del)      DATASET="TPCH_big_del" 
                 OUTPUT_FILE=${QUERY}_big_del.csv
                 ;;
   tiny)         DATASET="TPCH_tiny" 
                 OUTPUT_FILE=${QUERY}_tiny.csv
                 ;;
   tiny_del)     DATASET="TPCH_tiny_del" 
                 OUTPUT_FILE=${QUERY}_tiny_del.csv
                 ;;
   *)            DATASET="TPCH_standard"
                 OUTPUT_FILE=${QUERY}.csv
                 ;;
esac

echo "Running $QUERY on $DATASET"

sed -e "s:@@DATASET@@:$DATASET:g" $SCRIPT_DIR/queries/$QUERY.sql > _query.sql
psql -AqtF "," -f _query.sql -o $OUTPUT_FILE
rm -f _query.sql

echo "Done. The result is in $OUTPUT_FILE."
