#!/bin/bash

# This script should be run from within the Esper scripts/ dir.
#

if [ $# -ne 5 ]; then
  echo "Usage: $0 <timeout> <sample rate> <esper dir> <output dir> <data dir>"
  exit 1
fi

timeout=$1
sample_rate=$2
esper_dir=$3
output_dir=$4
data_dir=$5

if [ ! -d $esper_dir -o ! -f $esper_dir/src/org/dbtoaster/experiments/tpch/TPCHSkeleton.java ]; then
  echo "Invalid Esper directory: $esper_dir"
  exit 1
fi

if [ ! -d $data_dir ]; then
  echo "Invalid data dir: $data_dir"
  exit 1
fi

working_dir=`pwd`
query_dir=$esper_dir/queries/tpch
script_dir=$esper_dir/scripts
bin_dir=$esper_dir/bin

if [ ! -d $bin_dir -o ! -f $bin_dir/org/dbtoaster/experiments/tpch/TPCHSkeleton.class ]; then
  echo "Compiling Esper experiments"
  mkdir -p $bin_dir
  cd $esper_dir && ant compile
  cd $working_dir
fi

queries=`ls -1 $query_dir/*.esper | sed 's/.esper//'`

for i in 0; do
  # Prepare query files in a new run dir
  run_dir="$output_dir/run$i"
  test -d $run_dir || (echo "Creating run output: $run_dir" && mkdir -p $run_dir)
  
  if [ -L $bin_dir/data -o -f $bin_dir/data ]; then
    echo -n "Removing old dataset..."
    rm $bin_dir/data && echo "success" || echo "failed"    
  fi

  if [ ! -f $bin_dir/data ]; then
    echo "Linking dataset..."
    ln -s $data_dir $bin_dir/data

    for q in $queries; do
      query=`basename $q`
      if [ -f $query_dir/"$query".esper ]; then
        # Run the SPE on this query.
        # For TPCH, this requires a data dir relative to the bin_dir  
        echo "Running SPE on $query"
        $script_dir/run_esper.py $query -e $esper_dir -q $query_dir -d data -o $run_dir -t $timeout -s $sample_rate
        sleep 20 
      fi
    done
  fi
done