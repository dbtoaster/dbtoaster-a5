--alias CUSTOMER,C,ORDERS,O,LINEITEM,L
--slice transform ORDERS[5]~/([0-9]*)-.*/\1/
--slice transform LINEITEM[16]<d10,11
--slice transform LINEITEM[17]<d10,12
--slice transform LINEITEM[14]#
--slice project   LINEITEM(0,16,17,14)
--slice project   ORDERS(0,1,5,7)
--slice project   CUSTOMER(0,3)
--slice source test
--partition Map q on 5 weight by 1
--partition Map qLINEITEM1 on 2 weight by 1
create table customer(c_custkey int, c_nationkey int); 
create table orders(o_orderkey int, o_custkey int, o_opriority int, o_spriority int);
create table lineitem(l_orderkey int, l_lateship int, l_latecommit int, l_shipmode int);
select sum(1),c_custkey,c_nationkey,o_orderkey,o_opriority,o_spriority,l_lateship,l_latecommit,l_shipmode
from customer c, orders o, lineitem l
where o_custkey=c_custkey and l_orderkey=o_orderkey
group by c_custkey,c_nationkey,o_orderkey,o_opriority,o_spriority,l_lateship,l_latecommit,l_shipmode;