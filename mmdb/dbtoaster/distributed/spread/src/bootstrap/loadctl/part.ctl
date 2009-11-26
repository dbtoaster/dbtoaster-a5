LOAD DATA
INFILE '/home/yanif/datasets/tpch/100m/part.tbl'
INTO TABLE PART
APPEND
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
( p_partkey, p_name, p_mfgr, p_brand,
  p_type, p_size, p_container, p_retailprice,
  p_comment
)