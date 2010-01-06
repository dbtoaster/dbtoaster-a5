#!/bin/sh

dataset_dir=/home/yanif/datasets/tpch/
output_dir=tpch/
tmp_dir=maps/

if [ $# -lt 1 ]; then
    echo "Usage $0 [-d data dir] [-o output dir] [-t temp dir] <boot spec> <num nodes>"
    exit
fi

while getopts ":d:o:" option
do
    case $option in
      d)  dataset_dir=$OPTARG;;
      o)  output_dir=$OPTARG;;
      o)  tmp_dir=$OPTARG;;
    esac
done

shift $(($OPTIND - 1))

if [[ !( -d $dataset_dir && -d $output_dir && -d $tmp_dir ) ]]; then
    echo "Invalid directories: $dataset_dir, $output_dir, $tmp_dir"
    exit
fi

if [ ! -f $1 ]; then
    echo "No such file: $1"
    exit
fi

boot_file=$1
num_nodes=$2
mapnames=`awk '{ i=i+1; if (i%6 == 1) { names = names j $0; j = " " } } END { print names }' $boot_file`

# Print task
echo "Bootstrapping $boot_file"
echo "Dataset: $dataset_dir"
echo "Output dir: $output_dir"
echo "Temp dir: $tmp_dir"
echo "Maps: $mapnames"
echo "# nodes: $num_nodes"

for i in 1g 10g; do
    mv_query_file=${boot_file%.boot}_mv.sql
    mv_cleanup_file=${boot_file%.boot}_mv_cleanup.sql

    ./gen_ctl.sh $i
    sqlplus / @tpch_boot.sql 
    ./tpch_load.sh

    # One-time materialized view construction, that should be reused by queries issued in bootstrap.rb
    # Note: this requires Oracle Enterprise Edition.
    awk '{ i = i+1; if (i%6==1) { name = $0 }; if (i%6==3) { print "drop materialized view " name ";"; print "create materialized view " name " never refresh enable query rewrite as " $0 ";"} }' $boot_file > $mv_query_file && grep "drop materialized" $mv_query_file > $mv_cleanup_file

    sqlplus / @$mv_query_file

    rm -rf __db* maps/*
    for (( n=0; n<$num_nodes; n++ )); do
        for m in `echo $mapnames`; do
            time ../../bin/bootstrap.sh -d $dataset_dir/$i -s $n -m $m -o $tmp_dir $boot_file
        done
    done

    test -d $output_dir/$i || (echo "Creating $output_dir/$i" && mkdir -p $output_dir/$i/)
    mv __db* $output_dir/$i/
    mv maps/* $output_dir/$i/

    sqlplus / @$mv_cleanup_file
    rm $mv_query_file $mv_cleanup_file
done
