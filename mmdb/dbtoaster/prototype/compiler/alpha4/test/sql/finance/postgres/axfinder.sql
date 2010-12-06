DROP TABLE IF EXISTS bids;
CREATE TABLE bids(broker_id float, v float, p float);

DROP TABLE IF EXISTS asks;
CREATE TABLE asks(broker_id float, v float, p float);

COPY BIDS FROM '@@PATH@@/testdata/BIDS.dbtdat' WITH DELIMITER ',';
COPY ASKS FROM '@@PATH@@/testdata/ASKS.dbtdat' WITH DELIMITER ',';

SELECT   b.broker_id, sum(a.v + ((-1) * b.v))
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.p + ((-1) * b.p) > 1000) OR
           (b.p + ((-1) * a.p) > 1000) )
GROUP BY b.broker_id;

DROP TABLE bids;
DROP TABLE asks;