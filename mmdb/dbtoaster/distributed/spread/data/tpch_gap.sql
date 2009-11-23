--slice project CUSTOMERS(0,3,5)
create table customers(cid int, c_nid int, acctbal float);

-- Inner query for average per-nation customer wealth gap
select c1.cid, c1.c_nid, c2.c_nid,
       sum(c2.acctbal), sum(1)
from customers c1, customers c2
where c1.c_nid <> c2.c_nid
and   c1.acctbal > c2.acctbal
group by c1.cid, c1.c_nid, c2.c_nid;

-- TODO in query layer, where above query produces schema: R(ck,nk1,nk2,sab,cnt)
-- select R.nk1, R.nk2, sum(R.sab/R.cnt) from R group by R.nk1, R.nk2