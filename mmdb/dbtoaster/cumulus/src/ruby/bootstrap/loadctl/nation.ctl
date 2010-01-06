LOAD DATA
INFILE '/home/yanif/datasets/tpch/100m/nation.tbl'
INTO TABLE NATION
APPEND
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
( n_nationkey, n_name, n_regionkey, n_comment )