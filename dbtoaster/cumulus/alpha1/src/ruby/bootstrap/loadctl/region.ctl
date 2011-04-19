LOAD DATA
INFILE '/home/yanif/datasets/tpch/100m/region.tbl'
INTO TABLE REGION
APPEND
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
( r_regionkey, r_name, r_comment )