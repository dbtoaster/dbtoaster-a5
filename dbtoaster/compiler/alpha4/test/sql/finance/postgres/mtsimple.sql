DROP TABLE IF EXISTS InsertBids;
CREATE TABLE InsertBids(broker_id float, p float, v float);

DROP TABLE IF EXISTS InsertAsks;
CREATE TABLE InsertAsks(broker_id float, p float, v float);

DROP TABLE IF EXISTS DeleteBids;
CREATE TABLE DeleteBids(broker_id float, p float, v float);

DROP TABLE IF EXISTS DeleteAsks;
CREATE TABLE DeleteAsks(broker_id float, p float, v float);

COPY InsertBids FROM '@@PATH@@/testdata/InsertBIDS.dbtdat' WITH DELIMITER ',';
COPY InsertAsks FROM '@@PATH@@/testdata/InsertASKS.dbtdat' WITH DELIMITER ',';

COPY DeleteBids FROM '@@PATH@@/testdata/DeleteBIDS.dbtdat' WITH DELIMITER ',';
COPY DeleteAsks FROM '@@PATH@@/testdata/DeleteASKS.dbtdat' WITH DELIMITER ',';

SELECT broker_id, p, v INTO TABLE Bids FROM InsertBids
EXCEPT ALL SELECT broker_id, p, v FROM DeleteBids;

SELECT broker_id, p, v INTO TABLE Asks FROM InsertAsks
EXCEPT ALL SELECT broker_id, p, v FROM DeleteAsks;

select b.broker_id, sum(a.p*a.v+-1*b.p*b.v)
from  bids b, asks a
where 0.25*(select sum(b1.v) from bids b1) >
           (select case when v is null then 0 else v end
            from (select sum(b2.v) as v from bids b2 where b2.p > b.p) as R)
group by b.broker_id;

DROP TABLE InsertBids;
DROP TABLE InsertAsks;
DROP TABLE DeleteBids;
DROP TABLE DeleteAsks;
DROP TABLE Bids;
DROP TABLE Asks;