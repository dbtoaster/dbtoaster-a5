#!/bin/bash

if [ $# -ne 2 ]
then
  echo "usage `basename $0` <tpch dataset dir> <sample freq>"
  exit 1
fi

sample=$2

# Finance
./run_finance.sh ../queries/finance/vwap.esper -ib finance/InsertBids.dbtdat -db finance/DeleteBids.dbtdat -s $samplefreq -r 1
./run_finance.sh ../queries/finance/axfinder.esper -ib finance/InsertBIDS.dbtdat -db finance/DeleteBIDS.dbtdat -ia finance/InsertASKS.dbtdat -da finance/DeleteASKS.dbtdat -s $samplefreq -r 1
./run_finance.sh ../queries/finance/brokerspread.esper -ib finance/InsertBIDS.dbtdat -db finance/DeleteBIDS.dbtdat -s $samplefreq -r 1
./run_finance.sh ../queries/finance/pricespread.esper -ib finance/InsertBIDS.dbtdat -db finance/DeleteBIDS.dbtdat -ia finance/InsertASKS.dbtdat -da finance/DeleteASKS.dbtdat -s $samplefreq -r 1
./run_finance.sh ../queries/finance/missedtrades.esper -ib finance/InsertBIDS.dbtdat -db finance/DeleteBIDS.dbtdat -ia finance/InsertASKS.dbtdat -da finance/DeleteASKS.dbtdat -s $samplefreq -r 3

# TPCH
basedir=$1
./run_tpch.sh ../queries/tpch/query3.esper -i tpch/lineitem.csv -i tpch/customer.csv -i tpch/orders.csv -s $samplefreq -r 1 -b $basedir
./run_tpch.sh ../queries/tpch/query17.esper -i tpch/lineitem.csv -i tpch/part.csv -s $samplefreq -r 3 -b $basedir
./run_tpch.sh ../queries/tpch/query18.esper -i tpch/lineitem.csv -i tpch/orders.csv -i tpch/customer.csv -s $samplefreq -r 3 -b $basedir
./run_tpch.sh ../queries/tpch/query22.esper -i tpch/orders.csv -i tpch/customer.csv -s $samplefreq -r 3 -b $basedir
./run_tpch.sh ../queries/tpch/ssb4.esper -i tpch/orders.csv -i tpch/customer.csv -i tpch/supplier.csv -i tpch/nation.csv -i tpch/lineitem.csv -i tpch/part.csv -s $samplefreq -r 1 -b $basedir
