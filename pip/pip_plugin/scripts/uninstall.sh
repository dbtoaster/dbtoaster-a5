#!/bin/bash

DB='postgres'
if [ $1 ] ; then
  DB=$1
fi

psql $DB -f uninstall.sql  > /dev/null 2> status
grep -v NOTICE status
if [ -f installs/$DB.install.sql ] ; then
  mv installs/$DB.install.sql installs/$DB.install.sql.bak
fi
rm status
