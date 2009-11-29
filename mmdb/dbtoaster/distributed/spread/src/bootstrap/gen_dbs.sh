#!/bin/sh

dataset_dir=/home/yanif/datasets/tpch/
output_dir=tpch/
tmp_dir=maps/

if [ $# -lt 1 ]; then
    echo "Usage $0 [-d data dir] [-o output dir] [-t temp dir] <boot spec>"
    exit
fi

while getopts ":d:o:" option
do
    case $option in
      d)  test -d $OPTARG && dataset_dir=$OPTARG || echo "No such dataset dir $OPTARG";;
      o)  test -d $OPTARG && output_dir=$OPTARG || echo "No such output dir $OPTARG";;
      o)  test -d $OPTARG && tmp_dir=$OPTARG || echo "No such temporary dir $OPTARG";;
    esac
done

shift $(($OPTIND - 1))

if [ ! -f $1 ]; then
    echo "No such file: $1"
    exit
fi

boot_file=$1
mapnames=`awk '{ i=i+1; if (i%6 == 1) { names = names j $0; j = " " } } END { print names }' $boot_file`

# Print task
echo "Bootstrapping $boot_file"
echo "Dataset: $dataset_dir"
echo "Output dir: $output_dir"
echo "Temp dir: $tmp_dir"
echo "Maps: $mapnames"

for i in 1g 10g; do

    ./gen_ctl.sh $i
    sqlplus / @tpch_boot.sql 
    ./tpch_load.sh

    rm -rf __db* maps/*
    for n in 0 1 2 3 4 5 6 7; do
        for m in `echo $mapnames`; do
            time ./bootstrap.rb -d $dataset_dir/$i -n $n -m $m -o $tmp_dir $boot_file
        done
    done

    test -d $output_dir/$i || (echo "Creating $output_dir/$i" && mkdir -p $output_dir/$i/)
    mv __db* $output_dir/$i/
    mv maps/* $output_dir/$i/
done
