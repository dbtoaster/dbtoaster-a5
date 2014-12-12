#!/bin/bash

DBT_DIR=`pwd`
SCRIPT_DIR=`dirname $0`

for dataset in standard tiny big 
do
   sed -e "s:@@DBT_DIR@@:$DBT_DIR:g" $SCRIPT_DIR/schemas.sql | \
   sed -e "s:@@DATASET@@:$dataset:g" | \
   sed -e "s:@@FILE_SUFFIX@@::g"> _init.sql
   psql -d dbtoaster -f _init.sql
   rm -f _init.sql
done

for dataset in standard_del tiny_del big_del custom custom_huge
do
   sed -e "s:@@DBT_DIR@@:$DBT_DIR:g" $SCRIPT_DIR/schemas.sql | \
   sed -e "s:@@DATASET@@:$dataset:g" | \
   sed -e "s:@@FILE_SUFFIX@@:_active:g" > _init.sql
   psql -d dbtoaster -f _init.sql
   rm -f _init.sql
done

