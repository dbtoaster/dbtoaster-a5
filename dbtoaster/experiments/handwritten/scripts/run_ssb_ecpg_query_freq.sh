#!/bin/bash

export LD_LIBRARY_PATH=/home/yanif/software/postgresql/lib

for i in 7900000 3950000 2633000 1975000 1580000; do
    ./ssb ecpg $i out/ssb_ecpg_"$i".log results/ssb_ecpg_"$i".txt out/ssb_ecpg_"$i".stats /home/yanif/datasets/tpch/sf1/singlefile a > out/ssb_ecpg_"$i".dump 2>&1
done
