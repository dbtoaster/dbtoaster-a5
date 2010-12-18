-- Create final state of bids and asks table.

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

-- QUERY_1_1 
SELECT   b.broker_id, sum(a.v + ((-1) * b.v))
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.p + ((-1) * b.p) > 1000) OR
           (b.p + ((-1) * a.p) > 1000) )
GROUP BY b.broker_id;

/*
-- QUERY_1_1BIDS1_init
create view query_1_1BIDS1_init as
select broker_id, p, sum(v) as v from asks group by broker_id, p;

-- QUERY_1_1BIDS2_init
create view query_1_1BIDS2_init as
select broker_id, p, sum(1) as v from asks group by broker_id, p;

-- QUERY_1_1BIDS4
create view QUERY_1_1BIDS4 as
select s.p,broker_id, sum(r.v) as v
  from query_1_1BIDS2_init as r, 
  (select distinct p from bids) as s
where 1000 < (s.p-r.p)
group by s.p,broker_id; 

-- QUERY_1_1BIDS3
create view QUERY_1_1BIDS3 as
select s.p,broker_id, sum(r.v) as v
  from query_1_1BIDS1_init as r,
  (select distinct p from bids) as s
where 1000 < (s.p-r.p) 
group by s.p,broker_id;

-- QUERY_1_1BIDS2
create view QUERY_1_1BIDS2 as
select s.p,broker_id, sum(r.v) as v
  from query_1_1BIDS2_init as r,
  (select distinct p from bids) as s
where 1000 < (r.p-s.p)
group by s.p,broker_id;

-- QUERY_1_1BIDS1
create view QUERY_1_1BIDS1 as
select s.p,broker_id, sum(r.v) as v
  from query_1_1BIDS1_init as r,
  (select distinct p from bids) as s
where 1000 < (r.p-s.p) 
group by s.p,broker_id;

select r.broker_id,
  sum(a.v)-sum(r.v*b.v)
  --sum(c.v)+sum(a.v)-sum(r.v*d.v)-sum(r.v*b.v)
from bids as r,
  QUERY_1_1BIDS1 a,
  QUERY_1_1BIDS2 b
  --QUERY_1_1BIDS3 c,
  --QUERY_1_1BIDS4 d
where 
  a.p = r.p and b.p = r.p
  --and c.p = r.p and d.p = r.p
  and a.broker_id = r.broker_id and b.broker_id = r.broker_id
  --and c.broker_id = r.broker_id and d.broker_id = r.broker_id
group by r.broker_id;

DROP VIEW QUERY_1_1BIDS1;
DROP VIEW QUERY_1_1BIDS2;
DROP VIEW QUERY_1_1BIDS3;
DROP VIEW QUERY_1_1BIDS4;

DROP VIEW query_1_1BIDS1_init;
DROP VIEW query_1_1BIDS2_init;

-- QUERY_1_1ASKS2_init
--create view query_1_1ASKS2_init as
select broker_id, p, sum(v) as v from bids group by broker_id, p;

-- QUERY_1_1ASKS1_init
--create view query_1_1ASKS1_init as
select broker_id, p, sum(1) as v from bids group by broker_id, p;
*/

DROP TABLE InsertBids;
DROP TABLE InsertAsks;
DROP TABLE DeleteBids;
DROP TABLE DeleteAsks;
DROP TABLE Bids;
DROP TABLE Asks;