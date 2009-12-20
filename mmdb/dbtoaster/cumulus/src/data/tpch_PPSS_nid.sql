--slice project   PART(0,7)
--slice project   PARTSUPP(0,1,2,3)
--slice project   SUPPLIER(0,3)
--alias PART,P,PARTSUPP,PS,SUPPLIER,S
--xpartition Map q
--xpartition Map qPART1
--xpartition Map qPART2
--xpartition Map qSUPPLIER1
--xpartition Map qSUPPLIER2
--xpartition Map qPARTSUPP1
--xpartition Map qPARTSUPP2
--xpartition Map qPARTSUPP3
--xpartition Map qPART1SUPPLIER1
--xpartition Map qPART2SUPPLIER1
create table part(p_partkey int, p_retailprice float);
create table partsupp(ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost float);
create table supplier(s_suppkey int, s_nationkey int);
select 
    s.s_nationkey, sum((p.p_retailprice + (-1 * ps.ps_supplycost)) * ps.ps_availqty)
from
    part p, partsupp ps, supplier s
where
    p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey
group by
    s.s_nationkey;