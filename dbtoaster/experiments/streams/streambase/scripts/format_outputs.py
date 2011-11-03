#!/usr/bin/python

import csv, time
from sys import argv

with open(argv[1]) as csv_file:
  reader = csv.reader(csv_file)
  schema=reader.next()
  start = -1
  for row in reader:
    tstr=row[0].split('.')
    msec=tstr[1].split('-')[0]
    t=time.mktime(time.strptime(tstr[0], '%Y-%m-%d %H:%M:%S'))
    t += float(msec) / 1000
    if start < 0:
      start = t
    print str(t-start)+","+row[1]
