--persist
--partition q:6
create table customers(cid int, nid int); 
create table orders(oid int, o_cid int, opriority int, spriority int);
create table lineitems(l_oid int, lateship int, latecommit int, shipmode int);
select sum(1),nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode from customers,orders,lineitems where o_cid=cid and l_oid=oid group by nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode;