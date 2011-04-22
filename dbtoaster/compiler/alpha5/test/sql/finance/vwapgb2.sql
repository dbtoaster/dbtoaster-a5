CREATE TABLE bids(broker_id float, price float, volume float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

-- b1.price is an example of both upward and sideways propagation
SELECT b1.price, sum(b1.volume) 
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
       > (select sum(b2.volume) from bids b2 where b2.price > b1.price)
GROUP BY b1.price;
