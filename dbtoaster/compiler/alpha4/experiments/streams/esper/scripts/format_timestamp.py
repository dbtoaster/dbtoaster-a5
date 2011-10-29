#!/usr/bin/python

import sys

with open(sys.argv[1]) as f:
    start = -1
    for line in f:
        fields = line.strip().split(',')
        t = float(fields[0])
        tid = fields[1]
        t = t/1000
        if start < 0:
            start = t
        print str(t-start)+","+tid