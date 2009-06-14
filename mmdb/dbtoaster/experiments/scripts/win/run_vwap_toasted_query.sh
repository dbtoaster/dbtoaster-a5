#!/bin/bash

bindir="$(cygpath -w "/cygdrive/c/Documents and Settings/yanif/My Documents/Visual Studio 2008/Projects/vwap/Release/")"

vwapdir="c:/Temp/vwap/"
vwapshelldir=/cygdrive/c/Temp/vwap/

"$bindir/vwap.exe" toasted $vwapdir/vwapdata 1 $vwapdir/data/20081201.csv $vwapdir/out/vwap_dbt.log $vwapdir/results/vwap_dbt.txt $vwapdir/out/vwap_dbt.stats > $vwapshelldir/out/vwap_dbt.dump 2>&1
