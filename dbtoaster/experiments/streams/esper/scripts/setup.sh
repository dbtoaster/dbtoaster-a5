#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 <dbtoaster dir>"
  exit 1 
fi 

dbtoaster_dir=$1
esper_dir=$dbtoaster_dir/experiments/streams/esper
script_dir=$esper_dir/scripts

test -d $esper_dir/bin || mkdir -p $esper_dir/bin

# Finance symlinks
echo "Setting up finance data"
if [ ! -L $esper_dir/bin/finance ]; then
  ln -s $esper_dir/data/finance $esper_dir/bin/finance
fi

# TPCH symlinks
echo "Setting up TPCH data"
if [ ! -d $esper_dir/data/tpch ]; then
  test -d $esper_dir/data/tpch || mkdir -p $esper_dir/data/tpch
  cp -r $dbtoaster_dir/test/data/tpch $esper_dir/data/tpch/sf0.001
  cp -r $dbtoaster_dir/test/data/tpch_100M $esper_dir/data/tpch/sf0.1

  $script_dir/mk_esper_tpch_csv.sh $esper_dir/data/tpch/sf0.001
  $script_dir/mk_esper_tpch_csv.sh $esper_dir/data/tpch/sf0.1
fi

if [ ! -L $esper_dir/bin/tpch ]; then
  ln -s $esper_dir/data/tpch $esper_dir/bin/tpch
fi

cp $esper_dir/log4j.xml $esper_dir/bin/
