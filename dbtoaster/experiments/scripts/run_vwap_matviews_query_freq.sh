#!/bin/bash

#export LD_LIBRARY_PATH=/home/yanif/software/postgresql/lib

for i in 1480000 740000 493300 370000 296000; do
    ./vwap matviews /home/yanif/workspace/dbtoaster/src/examples/vwap/vwapdata $i data/20081201.csv out/vwap_mv_"$i".log results/vwap_mv_"$i".txt out/vwap_mv_"$i".stats vwap_triggers.sql > out/vwap_mv_"$i".dump 2>&1
done
