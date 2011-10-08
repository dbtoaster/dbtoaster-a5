#!/bin/bash
 
for i in `ls -1 *.tbl`; do
  fn=`basename $i | sed 's/.tbl//'`;

  # TPCH string can contain commas, replace them to "__" before replacing pipes to commas.
  sed 's/,/__/g; s/\|/,/g' < $i > $fn.csv;
done
