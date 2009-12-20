--alias ORDERS,O,LINEITEM,L,PART,P,PARTSUPP,PS,SUPPLIER,S,NATION,N
--slice transform PART[2]~/^[^#]*#([0-9]+)/\1/
--slice transform PART[4]~/^[^#]*#([0-9]+)/\1/
--slice transform PART[6]#
--slice transform LINEITEM[16]<d10,11
--slice transform LINEITEM[17]<d10,12
--slice transform LINEITEM[14]#
--slice project   LINEITEM(0,16,17,14,1,2)
--slice project   ORDERS(0,1,5,7)
--slice project   PART(0,2,4,5,6,7)
--slice project   PARTSUPP(0,1,2,3)
--slice project   CUSTOMER(0,3)
--slice project   SUPPLIER(0,3)
--slice project   NATION(1,3)
--slice source tpch/100m
create table customer(c_custkey int, foo int); 
create table orders(o_orderkey int, o_custkey int, o_opriority int, o_spriority int);
create table lineitem(l_orderkey int, l_lateship int, l_latecommit int, l_shipmode int, l_partkey int, l_suppkey int);
create table part(p_partkey int, p_mfgr int, p_type int, p_size int, p_container int, p_retailprice float);
create table partsupp(ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost float);
create table supplier(s_suppkey int, s_nationkey int);
create table nation(n_nationkey int, n_regionkey int);
select 
  sum(ps_availqty),
  l_lateship, p_type, p_size, p_container, l_shipmode, n_regionkey
from
  orders o, lineitem l, part p, partsupp ps, supplier s, nation n
where l_orderkey  = o_orderkey
AND   l_partkey   = p_partkey
AND   l_suppkey   = s_suppkey
AND   p_partkey   = ps_partkey
AND   s_suppkey   = ps_suppkey
AND   s_nationkey = n_nationkey
group by
  l_lateship, p_type, p_size, p_container, l_shipmode, n_regionkey;