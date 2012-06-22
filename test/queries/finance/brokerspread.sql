--  broker_id |      sum      
-- -----------+---------------
--          6 |  -55136926200
--          8 |   92711476900
--          2 |  186526996800
--          1 | -114956214400
--          3 |   85729885500
--          4 |   18596229300
--          5 |  158875988600
--          9 |  140073076900
--          7 |  -27166878800
--          0 |   78933620700

CREATE STREAM bids(t FLOAT, id INT, broker_id INT, volume FLOAT, price FLOAT)
  FROM FILE '../../experiments/data/finance/normal/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10', 
                            deterministic := 'yes');

SELECT x.broker_id, SUM((x.volume * x.price) - (y.volume * y.price)) AS bsp
FROM   bids x, bids y
WHERE  x.broker_id = y.broker_id AND x.t > y.t
GROUP BY x.broker_id;
