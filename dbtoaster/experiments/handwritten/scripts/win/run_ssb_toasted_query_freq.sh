#!/bin/bash

bindir="$(cygpath -w "/cygdrive/c/Documents and Settings/yanif/My Documents/Visual Studio 2008/Projects/ssb_unmanaged/x64/Release")"

ssbdir="c:/Temp/ssb/"
ssbshelldir=/cygdrive/c/Temp/ssb/

"$bindir/ssb_unmanaged.exe" toasted 1 $ssbdir/out/ssb_dbt.log $ssbdir/results/ssb_dbt.txt $ssbdir/out/ssb_dbt.stats $ssbdir/data a > $ssbshelldir/out/ssb_dbt.dump 2>&1
