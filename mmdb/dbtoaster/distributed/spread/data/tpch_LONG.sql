--slice transform PARTS[2]~/^[^#]*#([0-9]+)/\1/
--slice transform PARTS[3]~/^[^#]*#([0-9]+)/\1/
--slice transform PARTS[4]#
--slice transform PARTS[6]#
--slice project   PARTS(0,2,3,4,5,6)
--slice project   SUPP(0,3)
--slice project   PARTSUPP(0,1,2,3)
--slice transform LINEITEMS[16]<d10,11
--slice transform LINEITEMS[17]<d10,12
--slice transform LINEITEMS[14]#
--slice project   LINEITEMS(0,16,17,14,1,2)
--slice project   NATIONS(1,3)
--slice source tpch/100m
create table customers(cid int, foo int); 
create table orders(oid int, o_cid int, opriority int, spriority int);
create table lineitems(l_oid int, lateship int, latecommit int, shipmode int, l_pid int, l_sid int);
create table parts(pid int, mfgr int, type int, size int, container int, sellprice float);
create table partsupp(ps_pid int, ps_sid int, availqty int, buyprice float);
create table supp(sid int, s_nid int);
create table nations(nid int, rid int);
select 
  sum(availqty),
  lateship, type, size, container, shipmode, rid
from
  orders, lineitems, parts, partsupp, supp, nations
where
  l_oid=oid AND l_pid = pid AND l_sid = sid AND pid = ps_pid AND sid = ps_sid AND s_nid = nid
group by
  lateship, type, size, container, shipmode, rid;