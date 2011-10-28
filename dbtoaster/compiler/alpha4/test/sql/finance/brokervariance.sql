
CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10', 
                            deterministic := 'yes');

SELECT x.broker_id, SUM(x.volume * x.price * y.volume * y.price * 0.5)
FROM   bids x, bids y
WHERE  x.broker_id = y.broker_id
GROUP BY x.broker_id;