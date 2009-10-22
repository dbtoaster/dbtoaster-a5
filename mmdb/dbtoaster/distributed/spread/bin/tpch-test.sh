#!/bin/bash
#"

if [ "q18" = "$1" ] ; then
  ARGS='-u "ORDERS(0,1,5,7)" -t "ORDERS[5]~/([0-9]*)-.*/\1/" -u "CUSTOMERS(0,3)"  -u "LINEITEMS(0,16,17,14)" -t "LINEITEMS[16]<d10,11" -t "LINEITEMS[17]<d10,12" -t "LINEITEMS[14]!"'
fi
shift;

if [ "$1" ] ; then
  `dirname $0`/client.sh -q -s $ARGS < $1
else
  `dirname $0`/client.sh -q -h -s $ARGS
fi