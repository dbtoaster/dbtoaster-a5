#!/usr/bin/python

from __future__ import with_statement

import os, string, sys

if len(sys.argv) < 2:
    print "Usage: " + argv[0] + " <input file> <output file>"
    sys.exit(1)

bids = {}
asks = {}

bid_orders = {}
ask_orders = {}

bid_dups = {}
ask_dups = {}

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
                if not((price, int(volume)) in bids):
                    bids[(price, int(volume))] = (time,oid)
                    out.write(line)
                else:
                    bid_dups[oid] = (time, price, int(volume))

            elif action == "S":
                ask_orders[oid] = (time, price, int(volume))
                if not((price, int(volume)) in asks):
                    asks[(price, int(volume))] = (time,oid)
                    out.write(line)
                else:
                    ask_dups[oid] = (time, price, int(volume))

            # Deletions
            elif action == "D":
                if not((oid in bid_dups) or (oid in ask_dups)):
                    if oid in bid_orders:
                        (btime, bprice, bvol) = bid_orders[oid]
                        bids.pop(price, bvol)
                        out.write(line)
                    elif oid in ask_orders:
                        (atime, aprice, avol) = ask_orders[oid]
                        asks.pop(price, avol)
                        out.write(line)
                    else:
                        print "Unmatched deletion order " + oid

            # Full executions
            elif action == "F":
                if not((oid in bid_dups) or (oid in ask_dups)):
                    if oid in bid_orders:
                        (btime, bprice, bvol) = bid_orders[oid]
                        bids.pop(price, bvol)
                        out.write(line)
                    elif oid in ask_orders:
                        (atime, aprice, avol) = ask_orders[oid]
                        asks.pop(price, avol)
                        out.write(line)
                    else:
                        print "Unmatched full execution order " + oid

            # Partial executions
            elif action == "E":
                if not((oid in bid_dups) or (oid in ask_dups)):
                    if oid in bid_orders:
                        (btime, bprice, bvol) = bid_orders.pop(oid)
                        new_vol = bvol - int(volume)
                        bid_orders[oid] = (btime, bprice, new_vol)
                        bids.pop(bprice, bvol)
                        if not((bprice, new_vol) in bids):
                            bids[bprice,new_vol] = (time,oid)
                            out.write(line)
                    

                    elif oid in ask_orders:
                        (atime, aprice, avol) = ask_orders.pop(oid)
                        new_vol = avol - int(volume)
                        ask_orders[oid] = (atime, aprice, new_vol)
                        asks.pop(aprice, avol)
                        asks[aprice,new_vol] = (time,oid)
                        if not((aprice, new_vol) in asks):
                            asks[aprice,new_vol] = (time,oid)
                            out.write(line)


                    else:
                        print "Unmatched execution order " + oid

            #elif action == "X" or action == "C" or action == "T":

out.close()
