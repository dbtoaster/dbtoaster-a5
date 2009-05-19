#!/usr/bin/python

from __future__ import with_statement

import os, string, sys

if len(sys.argv) < 2:
    print "Usage: " + argv[0] + " <input file> <output file>"
    sys.exit(1)

bid_orders = {}
ask_orders = {}

out = open(sys.argv[2], 'w')

with open(sys.argv[1]) as f:
    for line in f:
            fields = line.split(',')
 
            time = fields[0]
            oid = fields[1]
            action = fields[2]
            volume = fields[3]
            price = fields[4].strip()

            if action == "B":
                bid_orders[oid] = (time, price, int(volume))
                out.write(line)

            elif action == "S":
                ask_orders[oid] = (time, price, int(volume))
                out.write(line)

            # Deletions
            elif action == "D":
                if oid in bid_orders:
                    (btime, bprice, bvol) = bid_orders[oid]
                    out.write(','.join([time, oid, "A", str(bvol), bprice])+'\n')
                elif oid in ask_orders:
                    (atime, aprice, avol) = ask_orders[oid]
                    out.write(','.join([time, oid, "G", str(avol), aprice])+'\n')
                else:
                    print "Unmatched deletion order " + oid

            # Full executions
            elif action == "F":
                if oid in bid_orders:
                    (btime, bprice, bvol) = bid_orders[oid]
                    out.write(','.join([time, oid, "H", str(bvol), bprice])+'\n')
                elif oid in ask_orders:
                    (atime, aprice, avol) = ask_orders[oid]
                    out.write(','.join([time, oid, "I", str(avol), aprice])+'\n')
                else:
                    print "Unmatched deletion order " + oid

            # Partial executions
            elif action == "E":
                if oid in bid_orders:
                    (btime, bprice, bvol) = bid_orders.pop(oid)
                    new_vol = bvol - int(volume)
                    bid_orders[oid] = (btime, bprice, new_vol)
                    out.write(','.join([time, price, "J", str(bvol), str(new_vol)])+'\n')                

                elif oid in ask_orders:
                    (atime, aprice, avol) = ask_orders.pop(oid)
                    new_vol = avol - int(volume)
                    ask_orders[oid] = (atime, aprice, new_vol)
                    out.write(','.join([time, price, "K", str(avol), str(new_vol)])+'\n')                

                else:
                    print "Unmatched execution order " + oid

            elif action == "X" or action == "C" or action == "T":
                out.write(line)

out.close()

