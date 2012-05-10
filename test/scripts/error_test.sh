#!/bin/bash

cd $(dirname $0)/../..;

ERRMSG=$(./bin/dbtoaster -r test/queries/error/$1.sql  2>&1 | grep "\[ERROR\]")

#echo "--->" $ERRMSG

if [ "$ERRMSG" ] ;
  then RESULT="SUCCESS";
  else RESULT="FAILURE";
fi

echo "Error expected in '$1':" $RESULT
if [ $RESULT = "FAILURE" ] ; 
  then exit -1;
fi