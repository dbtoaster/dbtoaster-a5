#!/bin/bash

java -cp ../bin:../lib/antlr-runtime-3.2.jar:../lib/cglib-nodep-2.2.jar:../lib/commons-beanutils-1.8.3.jar:../lib/commons-logging-1.1.1.jar:../lib/esper-4.3.0.jar:../lib/esperio-csv-4.3.0.jar:../lib/jopt-simple-3.3.jar:../lib/log4j-1.2.16.jar -Dlog4j.configuration=log4j.xml org.dbtoaster.experiments.finance.OrderBookSkeleton -q $@ &

pid=$!
echo $pid
wait $pid
