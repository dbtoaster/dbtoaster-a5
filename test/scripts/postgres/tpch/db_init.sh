#!/bin/bash

DBT_DIR=`pwd`
SCRIPT_DIR=`dirname $0`

FILES="standard.sql
tiny.sql
big.sql
standard_del.sql
tiny_del.sql
big_del.sql"

for f in $FILES
do
   sed -e "s:@@DBT_DIR@@:$DBT_DIR:g" $SCRIPT_DIR/init/$f > _init.sql
   psql -f _init.sql
   rm -f _init.sql
done
