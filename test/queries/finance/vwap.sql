/* Result on vwap5k:
     sum     
-------------
 28916017900
(1 row)
 */

CREATE STREAM bids(t FLOAT, id INT, broker_id INT, volume FLOAT, price FLOAT)
  FROM FILE '../../experiments/data/vwap100.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10', 
                            deterministic := 'yes');

SELECT sum(b1.price * b1.volume) AS vwap
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
            >
       (select sum(b2.volume) from bids b2 where b2.price > b1.price);
