CREATE TABLE bids(broker_id float, price float, volume float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

-- The following version causes a parser failure:
--   Fatal error: exception Sqlparser.SQLParseError
--     ("Ambiguous field BROKER_ID in relations B1,B2,B3")
-- This exception is unnecessary since attributes from B2 and B3 should not
-- be available in the outer scope. 
--SELECT broker_id, sum(b1.price * b1.volume)
--FROM   bids b1
--WHERE  0.25 * (select sum(b3.volume) from bids b3)
--       > (select sum(b2.volume) from bids b2 where b2.price > b1.price)
--group by broker_id;

-- b1.broker_id is an example of upward propagation only
-- b1.price is propagated sideways and marginalized (bigsum)
SELECT b1.broker_id, sum(b1.price * b1.volume) 
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
       > (select sum(b2.volume) from bids b2 where b2.price > b1.price)
GROUP BY b1.broker_id;
