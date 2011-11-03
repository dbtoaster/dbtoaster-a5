#!/bin/bash

# Produces timestamp, tid, and randomly samples 10% of the lines
logfile=$1
grep "t=" $logfile | sed 's/istream: {\(.*\)}/\1/; s/.*t=\([0-9]*\).*tid=\([0-9]*\).*/\1,\2/'
# | awk 'BEGIN {srand()} !/^$/ { if (rand() <= 0.1) print $0}'
