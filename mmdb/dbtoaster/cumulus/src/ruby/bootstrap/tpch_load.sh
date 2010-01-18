#!/bin/bash

oracle_user=cumulus
oracle_passwd=slicedbread

for i in `ls -1 loadctl/*.ctl` ; do
    sqlldr $oracle_user/$oracle_passwd rows=500000 direct=true control=$i;
done
