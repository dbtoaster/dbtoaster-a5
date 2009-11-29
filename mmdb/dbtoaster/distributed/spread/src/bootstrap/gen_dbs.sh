#!/bin/sh

for i in 1g 10g; do
    ./gen_ctl.sh $i
    sqlplus / @tpch_boot.sql 
    ./tpch_load.sh
    rm -rf __db* maps/*
    for n in 0 1 2 3 4 5 6 7; do
        for m in q qPARTSUPP2 qPARTSUPP3 qSUPPLIER1 qPART1 qSUPPLIER2 qPART2 qPART1SUPPLIER1 qPARTSUPP1 qPART2SUPPLIER1; do
            time ./bootstrap.rb -d /home/yanif/datasets/tpch/$i -n $n -m $m -o maps/ tpch_PPSS.boot
        done
    done
    mkdir tpch/$i
    mv __db* tpch/$i/
    mv maps/* tpch/$i/
done
