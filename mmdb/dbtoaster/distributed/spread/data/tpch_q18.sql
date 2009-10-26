--persist
--partition q:5
--partition qLINEITEMS1:2
--key CUSTOMERS[CID] <= ORDERS[O_CID]
--key ORDERS[OID] <= LINEITEMS[L_OID]
--node Alpha
--node Beta
--node Gamma
--node Delta
--node Epsilon
--#node Rho
--#node Bob
--#node Joe
create table customers(cid int, nid int); 
create table orders(oid int, o_cid int, opriority int, spriority int);
create table lineitems(l_oid int, lateship int, latecommit int, shipmode int);
select sum(1),nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode from customers,orders,lineitems where o_cid=cid and l_oid=oid group by nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode;