--alias SUPPLIER,S,PARTSUPP,PS,PART,P,CUSTOMER,C,LINEITEM,L,ORDERS,O
--slice project CUSTOMER(0,3)
--slice project ORDERS(0,1)
--slice project LINEITEM(0,3,4,5,6)
--slice project SUPPLIER(0,3)
--slice project PART(0,7)
--slice project PARTSUPP(0,1,2,3)
--slice source ~/tpch/100m
create table customer(c_custkey int, c_nationkey int);
create table orders(o_orderkey int, o_custkey int);
create table lineitem(l_orderkey int, l_linenumber int, l_quantity float, l_extendedprice float, l_discount float);
create table supplier(s_suppkey int, s_nationkey int);
create table part(p_partkey int, p_retailprice float);
create table partsupp(ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost float);

-- Compute "national debt", i.e. difference of inventory value
-- and customer spending in the same nation
select s_nationkey,
       sum(ps_availqty * (p_retailprice + (-1 * ps_supplycost)) +
           (-1 * (l_quantity * (l_extendedprice + (-1 * l_discount)))))
from
supplier s, partsupp ps, part p,
customer c, lineitem l, orders o
where s_nationkey = c_nationkey
and   s_suppkey = ps_suppkey
and   p_partkey = ps_partkey
and   c_custkey = o_custkey
and   o_orderkey = l_orderkey
group by s_nationkey
