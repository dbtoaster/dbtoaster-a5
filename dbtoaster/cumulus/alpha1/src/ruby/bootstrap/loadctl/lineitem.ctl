LOAD DATA
INFILE '/home/yanif/datasets/tpch/100m/lineitem.tbl'
INTO TABLE LINEITEM
APPEND
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
( l_orderkey,
  l_partkey,
  l_suppkey,
  l_linenumber,
  l_quantity,
  l_extendedprice,
  l_discount,
  l_tax,
  l_returnflag,
  l_linestatus,
  l_shipdate        DATE "YYYY-MM-DD",
  l_commitdate      DATE "YYYY-MM-DD",
  l_receiptdate     DATE "YYYY-MM-DD",
  l_shipinstruct,
  l_shipmode,
  l_comment
)