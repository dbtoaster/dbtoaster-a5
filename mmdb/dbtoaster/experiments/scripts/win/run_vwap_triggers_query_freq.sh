#!/bin/bash

bindir="$(cygpath -w "/cygdrive/c/Documents and Settings/yanif/My Documents/Visual Studio 2008/Projects/vwap/Release/")"

vwapdir="c:/Temp/vwap/"
vwapshelldir=/cygdrive/c/Temp/vwap/

for i in 1480000 740000 493300 370000 296000; do
    "$bindir/vwap.exe" triggers $vwapdir/vwapdata $i $vwapdir/data/20081201.csv $vwapdir/out/vwap_tg_"$i".log $vwapdir/results/vwap_tg_"$i".txt $vwapdir/out/vwap_tg_"$i".stats $vwapdir/win/mssql/vwap_triggers.sql > $vwapshelldir/out/vwap_tg_"$i".dump 2>&1
done
