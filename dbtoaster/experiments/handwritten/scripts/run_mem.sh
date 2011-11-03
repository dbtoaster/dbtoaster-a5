#!/bin/bash

export LD_LIBRARY_PATH=/home/yanif/software/postgresql/lib

#./vwap toasted /home/yanif/examples/vwapdata 1 data/20081201.csv out/vwap_dbt_mem.log results/vwap_dbt_mem.txt out/vwap_dbt_mem.stats > out/vwap_dbt_mem.dump 2>&1

./ssb toasted 1 out/ssb_dbt_mem.log results/ssb_dbt_mem.txt out/ssb_dbt_mem.stats /home/yanif/datasets/tpch/sf1/singlefile/ a > out/ssb_dbt_mem.dump 2>&1
