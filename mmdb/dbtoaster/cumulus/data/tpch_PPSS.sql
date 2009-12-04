--alias PART,P,PARTSUPP,PS,SUPPLIER,S
--slice transform PART[2]~/^[^#]*#([0-9]+)/\1/
--slice transform PART[4]~/^[^#]*#([0-9]+)/\1/
--slice transform PART[6]#
--slice project   PART(0,2,4,5,6,7)
--slice project   SUPPLIER(0,3)
--slice project   PARTSUPP(0,1,2,3)
--slice source ~/tpch/100m
--#partition Map qPARTS1SUPP1 on 0 weight by 2, 1 weight by 1
--#partition Map qPARTS2SUPP1 on 0 weight by 2, 1 weight by 1
--#partition Map qPARTS1 on 0 weight by 1, 1 into 2 pieces
--#partition Map qPARTS2 on 0 weight by 1, 1 into 2 pieces
--#partition Map qPARTSUPP2 on 0 weight by 1, 1 into 4 pieces
--#slice ratelimit 100
create table part(p_partkey int, p_mfgr int, p_type int, p_size int, p_container int, p_retailprice float);
create table partsupp(ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost float);
create table supplier(s_suppkey int, s_nationkey int);
select 
  s_nationkey, sum((p_retailprice + (-1 * ps_supplycost)) * ps_availqty)
from
  part p, partsupp ps, supplier s
where
  p_partkey = ps_partkey AND s_suppkey = ps_suppkey
group by
  s_nationkey;