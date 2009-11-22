--slice transform ORDERS[5]~/([0-9]*)-.*/\1/
--slice project   ORDERS(0,1,5,7)
--slice project   CUSTOMERS(0,3)
--slice transform LINEITEMS[16]<d10,11
--slice transform LINEITEMS[17]<d10,12
--slice transform LINEITEMS[14]#
--slice project   LINEITEMS(0,16,17,14)
--slice source test
--partition Map q on 5 weight by 1
--partition Map qLINEITEMS1 on 2 weight by 1
create table customers(cid int, nid int); 
create table orders(oid int, o_cid int, opriority int, spriority int);
create table lineitems(l_oid int, lateship int, latecommit int, shipmode int);
select sum(1),cid,nid,oid,opriority,spriority,lateship,latecommit,shipmode from customers,orders,lineitems where o_cid=cid and l_oid=oid group by cid,nid,oid,opriority,spriority,lateship,latecommit,shipmode;