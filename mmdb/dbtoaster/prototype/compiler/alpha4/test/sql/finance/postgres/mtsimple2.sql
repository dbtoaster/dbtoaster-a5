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

select b.broker_id, sum(a.v+b.v)
from bids b, asks a
where 0.25*(select sum(a1.v) from asks a1) >
           (select case when v is null then 0 else v end
            from (select sum(a2.v) as v from asks a2 where a2.p > a.p) as R)
and   0.25*(select sum(b1.v) from bids b1) >
           (select case when v is null then 0 else v end
            from (select sum(b2.v) as v from bids b2 where b2.p > b.p) as R)
group by b.broker_id;

/*
-- BS1_1
select p,sum(p*v) as bs1_1pv from asks group by p;

-- BS1_3, BS4_3
select sum(v) as bs1_3v from asks;

-- BS2_1
select broker_id,p,sum(1) as bs2_1c from bids group by broker_id,p;

-- BS2_3, BS3_3
select sum(v) as bs2_3v from bids;

-- BS3_1
select broker_id,p,sum(p*v) as bs3_1pv from bids group by broker_id,p;

-- BS4_1
select p,sum(1) as bs4_1c from asks group by p;

-- BS1_2, BS4_2
select pparam,sum(v) as bs1_2v
from (select distinct p as pparam from asks) as R, asks
where pparam<p group by pparam;

-- BS2_2, BS3_2
select pparam,sum(v) as bs2_2v
from (select distinct p as pparam from bids) as R, bids
where pparam<p group by pparam;
*/

DROP TABLE InsertBids;
DROP TABLE InsertAsks;
DROP TABLE DeleteBids;
DROP TABLE DeleteAsks;
DROP TABLE Bids;
DROP TABLE Asks;