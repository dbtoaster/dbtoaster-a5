#!/bin/sh

dataset_dir=/home/yanif/cumulus-pvldb/tpch/

while getopts ":d:" option
do
    case $option in
      d)  dataset_dir=$OPTARG;;
    esac
done

shift $(($OPTIND - 1))

if [ $# -lt 4 ]; then
    echo "Usage: $0 [-d data dir] <boot spec> <# new nodes> <# old nodes> <output node>"
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
output_node=$4
mapnames=`awk '{ i=i+1; if (i%6 == 1) { names = names j $0; j = " " } } END { print names }' $boot_file`

# Print task
echo "Repartitioning $boot_file: $old_num_nodes => $num_nodes, node $output_node"
echo "Dataset: $dataset_dir"
echo "Maps: $mapnames"

for i in 1g 10g; do
  rspec_file=${boot_file%.boot}_n"$num_nodes".rspec
  input_dir="$dataset_dir"/"$i" 
  output_dir="$dataset_dir"/"$i"_n"$num_nodes"

  test -d $input_dir || (echo "Failed to find input dir $input_dir" && exit)
  test -d $output_dir || (echo "Creating $output_dir" && mkdir -p $output_dir)

  test -f $rspec_file || \
    (echo "Creating repartition spec" && \
      ./gen_repartition_spec.rb $boot_file $num_nodes $rspec_file)
  
#  for m in `echo $mapnames`; do
    for (( j=0; j<$old_num_nodes; j++ )); do   
      echo "Repartitioning $j => $output_node"
      ./bootstrap.rb -l $j -s $output_node -r $rspec_file -d $input_dir -o $output_dir
    done
#  done
done
