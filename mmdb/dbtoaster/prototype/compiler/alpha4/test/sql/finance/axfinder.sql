CREATE TABLE bids(broker_id float, v float, p float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

CREATE TABLE asks(broker_id float, v float, p float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', validate := 'true', brokers := '10');

SELECT   b.broker_id, sum(a.v + ((-1) * b.v))
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.p + ((-1) * b.p) > 1000) OR
           (b.p + ((-1) * a.p) > 1000) )
GROUP BY b.broker_id;