#!/bin/sh

if [ $# -lt 1 ]; then
    echo "Usage $0 <MRToolkit job script>"
    exit 1
fi

MRTK_PATH=lib/mrtoolkit
JOB_PATH=`dirname $1`
RUN_CLASSPATH=lib/commons-logging-1.0.4.jar:lib/hadoop-0.20.1-core.jar:lib/hbase-0.20.2.jar:bin/bootstrap.jar

export JRUBY_CLASSPATH=`grep "jar" $1 | sed 's/.*extra\ \"\([^\"]*\)\"/\1/' | xargs basename | sed 's/\(.*\)/lib\/\1/' | xargs | sed 's/\ /:/g'`
export RUBYLIB=$MRTK_PATH:$JOB_PATH

jruby -J-cp $RUN_CLASSPATH -I $MRTK_PATH $1