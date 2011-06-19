/* Result on vwap5k:
 
 broker_id |   sum    
-----------+----------
         0 |   411957
         1 |   730602
         2 |   654330
         3 |  4165869
         4 |  7028680
         5 | -2443767
         6 | -1535632
         7 |  2362480
         8 |  1982830
         9 | -1755210
(10 rows)
*/

CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10');

CREATE TABLE asks(t float, id int, broker_id int, volume float, price float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10');

SELECT   b.broker_id, sum(a.volume + -1 * b.volume)
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.price + ((-1) * b.price) > 1000) OR
           (b.price + ((-1) * a.price) > 1000) )
GROUP BY b.broker_id;
