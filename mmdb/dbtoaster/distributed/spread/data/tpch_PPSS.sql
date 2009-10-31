--node Alpha@localhost:52982
--node Beta@localhost:52983
--node Gamma@localhost:52984
--node Delta@localhost:52985
--#key PARTS[PID] <= PARTSUPP[PS_PID]
--#key SUPP[SID] <= PARTSUPP[PS_SID]
--partition q:4
--domain PARTS=10000000,*,*,*
--domain SUPP=500000,25
create table parts(pid int, size int, container int, sellprice float);
create table partsupp(ps_pid int, ps_sid int, availqty int, buyprice float);
create table supp(sid int, s_nid int);
select 
  sum((sellprice + (-1 * buyprice)) * availqty),
  pid, sid, size, container, s_nid
from
  parts, partsupp, supp
where
  pid = ps_pid AND sid = ps_sid
group by
  pid, sid, size, container, s_nid;