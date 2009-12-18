LOAD DATA
INFILE '/home/yanif/datasets/tpch/100m/customer.tbl'
INTO TABLE CUSTOMER
APPEND
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
( c_custkey, c_name, c_address,
  c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment
)