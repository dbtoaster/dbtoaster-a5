#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 <data dir>"
  exit 1
fi

datadir=$1

if [ -f $datadir/lineitem.csv ]; then
  grep "|" $datadir/region.csv
  pcsv=$?
  if [ "x$pcsv" == "x0" ]; then
    for i in `ls -1 $datadir/*.csv`; do
      fn=`basename $i | sed 's/.csv//'`;

      # TPCH string can contain commas, replace them to "__" before replacing pipes to commas.
      sed 's/,/__/g; s/\|/,/g' < $i > $datadir/$fn.csv.new;
      mv $datadir/$fn.csv.new $datadir/$fn.csv;
    done
  fi
elif [ -f $datadir/lineitem.tbl ]; then
  for i in `ls -1 $datadir/*.tbl`; do
    fn=`basename $i | sed 's/.tbl//'`;

    # TPCH string can contain commas, replace them to "__" before replacing pipes to commas.
    sed 's/,/__/g; s/\|/,/g' < $i > $datadir/$fn.csv;
  done
fi
