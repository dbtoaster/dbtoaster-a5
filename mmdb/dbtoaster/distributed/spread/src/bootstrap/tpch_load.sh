#!/bin/bash

for i in `ls -1 loadctl/*.ctl` ; do
    sqlldr / rows=500000 direct=true control=$i;
done
