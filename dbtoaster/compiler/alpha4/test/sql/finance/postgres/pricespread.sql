DROP TABLE IF EXISTS InsertBids;
CREATE TABLE InsertBids(t float, order_id float, broker_id float, v float, p float);

DROP TABLE IF EXISTS InsertAsks;
CREATE TABLE InsertAsks(t float, order_id float, broker_id float, v float, p float);

DROP TABLE IF EXISTS DeleteBids;
CREATE TABLE DeleteBids(t float, order_id float, broker_id float, v float, p float);

DROP TABLE IF EXISTS DeleteAsks;
CREATE TABLE DeleteAsks(t float, order_id float, broker_id float, v float, p float);

COPY InsertBids FROM '@@PATH@@/testdata/InsertBIDS.dbtdat' WITH DELIMITER ',';
COPY InsertAsks FROM '@@PATH@@/testdata/InsertASKS.dbtdat' WITH DELIMITER ',';

COPY DeleteBids FROM '@@PATH@@/testdata/DeleteBIDS.dbtdat' WITH DELIMITER ',';
COPY DeleteAsks FROM '@@PATH@@/testdata/DeleteASKS.dbtdat' WITH DELIMITER ',';

SELECT broker_id, p, v INTO TABLE Bids FROM InsertBids
EXCEPT ALL SELECT broker_id, p, v FROM DeleteBids;

SELECT broker_id, p, v INTO TABLE Asks FROM InsertAsks
EXCEPT ALL SELECT broker_id, p, v FROM DeleteAsks;

-- look at spread between significant orders
select sum(a.p+-1*b.p) from bids b, asks a
where ( b.v>0.0001*(select sum(b1.v) from bids b1) )
and ( a.v>0.0001*(select sum(a1.v) from asks a1) );

DROP TABLE InsertBids;
DROP TABLE InsertAsks;
DROP TABLE DeleteBids;
DROP TABLE DeleteAsks;
DROP TABLE Bids;
DROP TABLE Asks;