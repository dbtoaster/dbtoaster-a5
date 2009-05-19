#!/bin/bash

./ssb toasted 1 out/ssb_dbt.log results/ssb_dbt.txt out/ssb_dbt.stats /home/yanif/datasets/tpch/sf1/singlefile/ a > out/ssb_dbt.dump 2>&1

for i in 7900000 2650000 1590000 1128700 877800 718200 607700 527800 464700 415800; do
    ./ssb naive $i out/ssb_naive_"$i".log results/ssb_naive_"$i".txt out/ssb_naive_"$i".stats /home/yanif/datasets/tpch/sf1/singlefile/ a > out/ssb_naive_"$i".dump 2>&1
done
