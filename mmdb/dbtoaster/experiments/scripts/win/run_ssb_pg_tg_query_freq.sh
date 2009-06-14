#!/bin/bash

bindir="$(cygpath -w "/cygdrive/c/Documents and Settings/yanif/My Documents/Visual Studio 2008/Projects/ssb/Release")"

ssbdir="c:/Temp/ssb/"
ssbshelldir=/cygdrive/c/Temp/ssb/

#for i in 7900000 3950000 2633000 1975000 1580000; do
for i in 7900000; do
    "$bindir/ssb.exe" pg_triggers $i $ssbdir/out/ssb_pg_tg_"$i".log $ssbdir/results/ssb_pg_tg_"$i".txt $ssbdir/out/ssb_pg_tg_"$i".stats $ssbdir/data a $ssbdir/win/postgres/ssb_triggers.sql > $ssbshelldir/out/ssb_pg_tg_"$i".dump 2>&1
done
