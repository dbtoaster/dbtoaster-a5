#!/bin/bash

for i in `ls -1 *.tbl`; do cat $i | sed 's/\(.*\)/1\|1\|\1/' > $i.evt; done
for i in `ls -1 *.tbl`; do tail -r $i | sed 's/\(.*\)/2\|0\|\1/' >> $i.evt; done
for i in `ls -1 *.tbl`; do mv $i.evt $i; done
