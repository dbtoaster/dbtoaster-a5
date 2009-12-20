--slice project CUSTOMER(0,3)
--slice project SUPPLIER(0,3)
--slice project ORDERS(0,1)
--slice project LINEITEM(0,1,2,3,4)
--alias CUSTOMER,C,SUPPLIER,S,ORDERS,O,LINEITEM,L
--slice source ~/tpch/100m
create table customer(c_custkey int, c_nationkey int);
create table supplier(s_suppkey int, s_nationkey int);
create table orders(o_orderkey int, o_custkey int);
create table lineitem(l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity float);

-- Compute national demand per part, i.e. the total quantity bought
-- and sold by suppliers and customers in the same nation
select l_partkey, sum(l_quantity)
from customer c, supplier s, orders o, lineitem l
where c_nationkey = s_nationkey
and   s_suppkey   = l_suppkey
and   c_custkey   = o_orderkey
and   o_orderkey  = l_orderkey
group by l_partkey;

-- TODO: second aggregate query for final averaging.
-- Weights national demand per part by total amount of the part sold.
--select l_partkey, sum(l_quantity)
--from lineitem l
--group by l_partkey
