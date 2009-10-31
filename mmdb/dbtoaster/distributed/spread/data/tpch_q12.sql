--persist
--partition q:6
--partition qLINEITEMS1:0
--key CUSTOMERS[CID] <= ORDERS[O_CID]
--key ORDERS[OID] <= LINEITEMS[L_OID]
--node Alpha@localhost:52982
--node Beta@localhost:52983
--node Gamma@localhost:52984
--node Delta@localhost:52985
--node Epsilon@localhost:52986
--#node Rho@localhost:52987
--#node Bob@localhost:52988
--#node Joe@localhost:52989
--#slice transform ORDERS[0]%100000
--#slice transform ORDERS[1]%100000
--slice transform ORDERS[5]~/([0-9]*)-.*/\1/
--slice project   ORDERS(0,1,5,7)
--#slice transform CUSTOMERS[0]%100000
--#slice transform CUSTOMERS[3]%100000
--slice project   CUSTOMERS(0,3)
--#slice transform LINEITEMS[0]%100000
--slice transform LINEITEMS[16]<d10,11
--slice transform LINEITEMS[17]<d10,12
--slice transform LINEITEMS[14]!
--slice project   LINEITEMS(0,16,17,14)
--domain CUSTOMERS=7500000,25
--domain ORDERS=300000000,*,10,*
--domain LINEITEMS=*,2,2,100
--source tpch_100m
create table customers(cid int, nid int); 
create table orders(oid int, o_cid int, opriority int, spriority int);
create table lineitems(l_oid int, lateship int, latecommit int, shipmode int);
select sum(1),nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode from customers,orders,lineitems where o_cid=cid and l_oid=oid group by nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode;