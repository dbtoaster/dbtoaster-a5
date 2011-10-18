#!/bin/bash

if [ $# -ne 4 ]; then
  echo "Usage: $0 <timeout> <query template dir> <output dir> <data dir>"
  exit 1
fi

timeout=$1
query_template_dir=$2
output_dir=$3
data_dir=$4

working_dir=`pwd`
script_dir=/home/yna/sbworkspace/scripts

queries=`ls -1 $query_template_dir/*_separate_loader.ssql | sed 's/_separate_loader.ssql//'`

for i in 0; do
  # Prepare query files in a new run dir
  run_dir="$output_dir/run$i"
  test -d $run_dir || (echo "Creating run output: $run_dir" && mkdir -p $run_dir)
  
  echo "Linking dataset..."
  ln -s $data_dir $run_dir/data

  for q in $queries; do
    query=`basename $q`
    if [ -f $query_template_dir/"$query"_separate_loader.ssql ]; then
      echo "Generating $query.ssql..."
      sed "s/@@PATH@@/data\//" < $query_template_dir/"$query"_separate_loader.ssql \
        > $run_dir/"$query"_separate_loader.ssql
      cp $query_template_dir/"$query".ssql $run_dir/"$query".ssql
    
      # Run the SPE on this query.
      cd $run_dir
      echo "Running SPE on $query"
      $script_dir/run_spe.py -t $timeout -l "_separate_loader" $query
      
      cd $working_dir
    fi
  done
done