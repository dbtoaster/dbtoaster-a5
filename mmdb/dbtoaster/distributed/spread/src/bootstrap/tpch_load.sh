#!/bin/bash

for i in `ls -1 loadctl/` ; do
    sqlldr / rows=100000 direct=true control=loadctl/$i;
done
