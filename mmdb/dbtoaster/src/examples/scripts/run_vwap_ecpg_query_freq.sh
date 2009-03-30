#!/bin/bash

export LD_LIBRARY_PATH=/home/yanif/software/postgresql/lib

for i in 1480000 740000 493300 370000 296000; do
    ./vwap ecpg /home/yanif/examples/vwapdata $i data/20081201.csv out/vwap_ecpg_"$i".log results/vwap_ecpg_"$i".txt out/vwap_ecpg_"$i".stats > out/vwap_ecpg_"$i".dump 2>&1
done
