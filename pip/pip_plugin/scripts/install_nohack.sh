#!/bin/bash

DB='postgres'
if [ $1 ] ; then
  DB=$1
fi

psql $DB -f uninstall.sql > /dev/null 2> ./status
#cat status | grep -v NOTICE
cat install.sql | grep -v "HACKED_SQL_ONLY" > tmp.nohack.install.sql
psql $DB -f tmp.nohack.install.sql > /dev/null 2> ./status
rm tmp.nohack.install.sql
cat status | grep -v NOTICE
rm status;
