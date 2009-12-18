LOAD DATA
INFILE '/home/yanif/datasets/tpch/100m/orders.tbl'
INTO TABLE ORDERS
APPEND
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
( o_orderkey,
  o_custkey,        
  o_orderstatus,
  o_totalprice,
  o_orderdate       DATE "YYYY-MM-DD",
  o_orderpriority,
  o_clerk,
  o_shippriority,
  o_comment
)