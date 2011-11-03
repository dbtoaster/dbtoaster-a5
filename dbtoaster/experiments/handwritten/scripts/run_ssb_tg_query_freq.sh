#!/bin/bash

#export LD_LIBRARY_PATH=/home/yanif/software/postgresql/lib

for i in 7900000 3950000 2633000 1975000 1580000; do
    ./ssb triggers $i out/ssb_tg_"$i".log results/ssb_tg_"$i".txt out/ssb_tg_"$i".stats /home/yanif/datasets/tpch/sf1/singlefile a ssb_triggers.sql > out/ssb_tg_"$i".dump 2>&1
done
