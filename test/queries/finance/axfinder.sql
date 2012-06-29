--  Result on vwap5k (with deterministic := 'yes'):
--  
--  broker_id |   sum    
-- -----------+----------
--          0 |  2446668
--          1 |  -648039
--          2 | -5363809
--          3 |   864240
--          4 |  8384852
--          5 |  3288320
--          6 | -2605617
--          7 |   243551
--          8 |  1565128
--          9 |   995180
-- (10 rows)


CREATE STREAM bids(t FLOAT, id INT, broker_id INT, volume FLOAT, price FLOAT)
  FROM FILE '../../experiments/data/finance/standard/vwap.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10', 
                            deterministic := 'yes');

CREATE STREAM asks(t FLOAT, id INT, broker_id INT, volume FLOAT, price FLOAT)
  FROM FILE '../../experiments/data/finance/standard/vwap.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10', 
                            deterministic := 'yes');

SELECT   b.broker_id, SUM(a.volume + (-1 * b.volume)) AS axfinder
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.price + ((-1) * b.price) > 1000) OR
           (b.price + ((-1) * a.price) > 1000) )
GROUP BY b.broker_id;
