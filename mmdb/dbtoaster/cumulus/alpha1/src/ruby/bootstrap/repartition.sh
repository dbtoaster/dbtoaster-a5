#!/bin/sh

dataset_dir=/home/yanif/cumulus-pvldb/tpch/

while getopts ":d:" option
do
    case $option in
      d)  dataset_dir=$OPTARG;;
    esac
done

shift $(($OPTIND - 1))

if [ $# -lt 3 ]; then
    echo "Usage: $0 [-d data dir] <boot spec> <# new nodes> <# old nodes>"
    exit
fi

if [ ! -d $dataset_dir ]; then
    echo "Invalid dataset dir: $dataset_dir"
    exit
fi

if [ ! -f $1 ]; then
    echo "Invalid file: $1"
    exit
fi

boot_file=$1
num_nodes=$2
old_num_nodes=$3
mapnames=`awk '{ i=i+1; if (i%6 == 1) { names = names j $0; j = " " } } END { print names }' $boot_file`

# Print task
echo "Repartitioning $boot_file: $old_num_nodes => $num_nodes"
echo "Dataset: $dataset_dir"
echo "Maps: $mapnames"

for i in 1g 10g; do
  rspec_file=${boot_file%.boot}_n"$num_nodes".rspec
  input_dir="$dataset_dir"/"$i" 
  output_dir="$dataset_dir"/"$i"_n"$num_nodes"

  test -d $input_dir || (echo "Failed to find input dir $input_dir" && exit)
  test -d $output_dir || (echo "Creating $output_dir" && mkdir -p $output_dir)

  ../../../bin/gen_rpspec.sh $boot_file $num_nodes $rspec_file
  
#  for m in `echo $mapnames`; do
    for (( j=0; j<$old_num_nodes; j++ )); do   
      for (( k=0; k<$num_nodes; k++ )); do
        echo "Repartitioning $j => $k"
          ../../../bin/bootstrap.sh -l $j -s $k -r $rspec_file -d $input_dir -o $output_dir
      done
    done

# Per-map repartitioning
#     ../../../bin/bootstrap.sh -l $j -r $rspec_file -d $input_dir -o $output_dir
#  done

# Per-dataset repartitioning
#  ../../../bin/bootstrap.sh -r $rspec_file -d $input_dir -o $output_dir

    mv __db* $output_dir/
done
