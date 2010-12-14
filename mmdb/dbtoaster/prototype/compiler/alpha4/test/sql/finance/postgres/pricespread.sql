DROP TABLE IF EXISTS bids;
CREATE TABLE bids(broker_id float, v float, p float);

DROP TABLE IF EXISTS asks;
CREATE TABLE asks(broker_id float, v float, p float);

COPY BIDS FROM '@@PATH@@/testdata/BIDS.dbtdat' WITH DELIMITER ',';
COPY ASKS FROM '@@PATH@@/testdata/ASKS.dbtdat' WITH DELIMITER ',';

-- look at spread between significant orders
select sum(a.p+((-1)*b.p)) from bids b, asks a
where ( b.v>0.0001*(select sum(b1.v) from bids b1) )
and ( a.v>0.0001*(select sum(a1.v) from asks a1) );

DROP TABLE bids;
DROP TABLE asks;