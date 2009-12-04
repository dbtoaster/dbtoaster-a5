LOAD DATA
INFILE '/home/yanif/datasets/tpch/100m/partsupp.tbl'
INTO TABLE PARTSUPP
APPEND
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
( ps_partkey, ps_suppkey,
  ps_availqty, ps_supplycost, ps_comment        
)