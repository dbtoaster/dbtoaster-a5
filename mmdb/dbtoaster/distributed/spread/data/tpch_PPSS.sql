--node Alpha
--node Beta
--node Gamma
--node Delta
--key PARTS[PID] <= PARTSUPP[PS_PID]
--key SUPP[SID] <= PARTSUPP[PS_SID]
create table parts(pid int, size int, container int, sellprice float);
create table partsupp(ps_pid int, ps_sid int, availqty int, buyprice float);
create table supp(sid int, s_nid int, acctbal float);
select 
  sum((sellprice + (-1 * buyprice)) * availqty),
  pid, sid, size, container, s_nid
from
  parts, partsupp, supp
where
  pid = ps_pid AND sid = ps_sid
group by
  pid, sid, size, container, s_nid;