--slice project CUSTOMERS(0,3)
--slice project ORDERS(0,1)
--slice project LINEITEMS(0,3,4,5,6)
--slice project SUPP(0,3)
--slice project PARTS(0,7)
--slice project PARTSUPP(0,1,2,3)

create table customers(cid int, c_nid int);
create table orders(oid int, o_cid int);
create table lineitems(l_oid int, linenumber int, quantity float, extendedprice float, discount float);
create table supp(sid int, s_nid int);
create table parts(pid int, retailprice float);
create table partsupp(ps_pid int, ps_sid int, availqty int, supplycost float);

-- Compute "national debt", i.e. difference of inventory value
-- and customer spending in the same nation
select s.s_nid,
       sum(ps.availqty * (p.retailprice + (-1 * ps.supplycost)) +
           (-1 * (l.quantity * (l.extendedprice + (-1 * l.discount)))))
from
supp s, partsupp ps, parts p,
customers c, lineitems l, orders o
where s.s_nid = c.c_nid
and   s.sid = ps.ps_sid
and   p.pid = ps.ps_pid
and   c.cid = o.o_cid
and   o.oid = l.l_oid
group by s.s_nid
