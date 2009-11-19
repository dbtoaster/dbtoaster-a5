--slice transform PARTS[2]~/^[^#]*#([0-9]+)/\1/
--slice transform PARTS[3]~/^[^#]*#([0-9]+)/\1/
--slice transform PARTS[4]#
--slice transform PARTS[6]#
--slice project   PARTS(0,2,3,4,5,6)
--slice project   SUPP(0,3)
--slice project   PARTSUPP(0,1,2,3)
--slice source tpch/100m
--partition Map q on 6 weight by 1
create table parts(pid int, mfgr int, type int, size int, container int, sellprice float);
create table partsupp(ps_pid int, ps_sid int, availqty int, buyprice float);
create table supp(sid int, s_nid int);
select 
  sum((sellprice + (-1 * buyprice)) * availqty),
  pid, mfgr, type, sid, size, container, s_nid
from
  parts, partsupp, supp
where
  pid = ps_pid AND sid = ps_sid
group by
  pid, mfgr, type, sid, size, container, s_nid;