#!/bin/bash

echo "Initializing install..."

DB='postgres'
if [ $1 ] ; then
  DB=$1
fi
OLDINSTALL='installs/'$DB'.install.sql';

if [ ! -d './installs' ] ; then
  mkdir installs
fi

if [ ! -f $OLDINSTALL ] ; then
  touch $OLDINSTALL
fi

echo "Computing upgrade requirements..."

diff install.sql $OLDINSTALL > upgrade_diff
rm -rf upgrade.sql
grep '^>' upgrade_diff | sed 's/^> *//' > upgrade.sql.tmp
scripts/gen_uninstall.sh upgrade.sql.tmp > upgrade.del.sql
rm upgrade.sql.tmp
grep '^<' upgrade_diff | sed 's/^< *//' > upgrade.inst.sql
rm upgrade_diff

DROPALL="no";

if   [ "0" != `grep "DROP FUNCTION" upgrade.del.sql | grep "_out" | wc -l` ] ; then
  DROPALL="yes"
elif [ "0" != `grep "DROP FUNCTION" upgrade.del.sql | grep "_in" | wc -l` ] ; then
  DROPALL="yes"
elif [ "0" != `grep "DROP TYPE" upgrade.del.sql | wc -l` ] ; then
  DROPALL="yes"
fi

if [ DROPALL = "yes" ] ; then
  echo "----------------------------------------------------------------------"
  echo "|WARNING| Re-running the installer will INVALIDATE YOUR DATA |WARNING|"
  echo "----------------------------------------------------------------------"
  echo "Code changes require a complete reinstall of the pip_plugin. This will"
  echo "invalidate any probabilistic data you have in your database. Please"
  echo "export your database and run the installer again."
  echo "----------------------------------------------------------------------"
  mv $OLDINSTALL $OLDINSTALL.bak
  cat $OLDINSTALL.bak | sed 's/^/ /' > $OLDINSTALL
  exit;
fi

if [ "0" != `cat upgrade.del.sql | wc -l` ] ; then
  echo "Removing old code (this may trigger errors; it's safe to ignore them)..."
  psql $DB -f upgrade.del.sql > /dev/null 2> ./status
  cat status | grep -v NOTICE
  echo "----- Ignore any errors above this line -----";
fi

echo "Installing..."
psql $DB -f upgrade.inst.sql > /dev/null 2> ./status
cat status | grep -v NOTICE

if [ "0" = `grep ERROR status | wc -l` ] ; then
  cp install.sql $OLDINSTALL  
  rm status
  if [ whoami != 'xthemage' ] ; then
    rm upgrade.del.sql upgrade.inst.sql
  fi
  echo "Success!  The most current PIP is now installed in DB '$DB'"
else
  echo "An error occurred installing PIP in DB '$DB'";
fi


