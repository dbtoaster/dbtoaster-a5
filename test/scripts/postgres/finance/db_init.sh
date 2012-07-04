#!/bin/bash

DBT_DIR=`pwd`
SCRIPT_DIR=`dirname $0`

DATASETS="standard
tiny
big
huge"

for dataset in $DATASETS
do
   sed -e "s:@@DBT_DIR@@:$DBT_DIR:g" $SCRIPT_DIR/schemas.sql | \
   sed -e "s:@@DATASET@@:$dataset:g" > _init.sql
   psql -f _init.sql
   rm -f _init.sql
done
