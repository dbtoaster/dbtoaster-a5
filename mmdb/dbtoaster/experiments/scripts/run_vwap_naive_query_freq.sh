#!/bin/bash

./vwap toasted /home/yanif/examples/vwapdata 1 data/20081201.csv out/vwap_dbt.log results/vwap_dbt.txt out/vwap_dbt.stats > out/vwap_dbt.dump 2>&1

#for i in 148000 74000 55500 37000 29600; do
#for i in 24670 18500 14800 12330 10570 9250 8220 7400; do
#    ./vwap naive /home/yanif/examples/vwapdata $i data/20081201.csv out/vwap_naive_"$i".log results/vwap_naive_"$i".txt out/vwap_naive_"$i".stats > out/vwap_naive_"$i".dump 2>&1
#done
