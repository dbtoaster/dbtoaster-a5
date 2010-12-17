DROP TABLE IF EXISTS bids;
CREATE TABLE bids(broker_id float, v float, p float);

DROP TABLE IF EXISTS asks;
CREATE TABLE asks(broker_id float, v float, p float);

COPY BIDS FROM '@@PATH@@/testdata/BIDS.dbtdat' WITH DELIMITER ',';
COPY ASKS FROM '@@PATH@@/testdata/ASKS.dbtdat' WITH DELIMITER ',';

select b.broker_id, sum(a.p*a.v+-1*b.p*b.v)
from  bids b, asks a
where 0.25*(select sum(b1.v) from bids b1) >
           (select case when v is null then 0 else v end
            from (select sum(b2.v) as v from bids b2 where b2.p > b.p) as R)
group by b.broker_id;

DROP TABLE bids;
DROP TABLE asks;