--persist
--partition q:5
--partition qLINEITEMS1:2
--#key CUSTOMERS[CID] <= ORDERS[O_CID]
--#key ORDERS[OID] <= LINEITEMS[L_OID]
--switch wl03.cac.cornell.edu
--node Node01@wl04.cac.cornell.edu:52982
--node Node02@wl05.cac.cornell.edu:52982
--node Node03@wl06.cac.cornell.edu:52982
--node Node04@wl07.cac.cornell.edu:52982
--node Node05@wl08.cac.cornell.edu:52982
--node Node06@wl09.cac.cornell.edu:52982
--node Node07@wl10.cac.cornell.edu:52982
--node Node08@wl11.cac.cornell.edu:52982
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
--slice source tpch/100m
create table customers(cid int, nid int); 
create table orders(oid int, o_cid int, opriority int, spriority int);
create table lineitems(l_oid int, lateship int, latecommit int, shipmode int);
select sum(1),nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode from customers,orders,lineitems where o_cid=cid and l_oid=oid group by nid,cid,oid,opriority,spriority,lateship,latecommit,shipmode;