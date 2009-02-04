#!/bin/bash

SOURCE=`dirname $0`/../src/pip.source

if [ $1 ] ; then
  SOURCE=$1
fi

cat $SOURCE | 
  grep "CREATE OPERATOR" | 
  sed 's/CREATE OPERATOR/DROP OPERATOR IF EXISTS/;
       s/PROCEDURE=[^,]*,//;
       s/LEFTARG=\([^,]*,\)/\1/;
         s/LEFTARG=\([^)]*\)/\1,NONE/;
      s/\(,[^,]*\)RIGHTARG=/\1/;
        s/RIGHTARG=/NONE,/;
      s/;/ CASCADE;/';
cat $SOURCE | 
  grep "CREATE" | 
  grep -v "CREATE TYPE" | 
  grep -v "CREATE OPERATOR" | 
  grep -v "CREATE SEQUENCE" | 
  sort | 
  sed 's/CREATE \([A-Za-z]*\) *\([^(]*([^)]*)\).*/DROP \1 IF EXISTS \2 CASCADE;/' 
cat $SOURCE | 
  grep "CREATE TYPE.*;" | 
  sed 's/CREATE TYPE \([^;]*\)/DROP TYPE IF EXISTS \1 CASCADE/';
cat $SOURCE | 
  grep "CREATE SEQUENCE" | 
  sed 's/CREATE SEQUENCE/DROP SEQUENCE IF EXISTS/;s/;/ CASCADE;/';
