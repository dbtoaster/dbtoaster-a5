--slice project CUSTOMERS(0,3)
--slice project SUPP(0,3)
--slice project ORDERS(0,1)
--slice project LINEITEMS(0,1,2,3,4)
create table customers(cid int, c_nid int);
create table supp(sid int, s_nid int);
create table orders(oid int, o_cid int);
create table lineitems(l_oid int, l_pid int, l_sid int, linenumber int, quantity float);

-- Compute national demand per part, i.e. the total quantity bought
-- and sold by suppliers and customers in the same nation
select l.l_pid, sum(l.quantity) from
    customers c, supp s,
    orders o, lineitems l
where
    c.c_nid = s.s_nid
and s.sid = l.l_sid
and c.cid = o.o_cid
and o.oid = l.l_oid
group by l.l_pid;

-- TODO: second aggregate query for final averaging.
-- Weights national demand per part by total amount of the part sold.
--select l.l_pid, sum(l.quantity)
--from lineitem l
--group by l.l_pid
