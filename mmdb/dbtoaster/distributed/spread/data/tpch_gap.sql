--alias CUSTOMER,C1,CUSTOMER,C2
--slice project CUSTOMER(0,3,5)
create table customer(c_custkey int, c_nationkey int, c_acctbal float);

-- Inner query for average per-nation customer wealth gap
select c1.c_custkey, c1.c_nationkey, c2.c_nationkey,
       sum(c2.c_acctbal), sum(1)
from customer c1, customer c2
where c1.c_nationkey <> c2.c_nationkey
and   c1.c_acctbal > c2.c_acctbal
group by c1.c_custkey, c1.c_nationkey, c2.c_nationkey;

-- TODO in query layer, where above query produces schema: R(ck,nk1,nk2,sab,cnt)
-- select R.nk1, R.nk2, sum(R.sab/R.cnt) from R group by R.nk1, R.nk2