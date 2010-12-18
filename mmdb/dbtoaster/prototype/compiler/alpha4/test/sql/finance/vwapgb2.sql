/* Result on vwap5k:
 price  |  sum  
--------+-------
 194500 |  1300
 194600 |   285
 196900 |  1300
 199300 |   500
 195400 |  1000
 196800 |  2200
 195100 |  2500
 198400 | 10150
 197500 |  7000
 196600 |   399
 199100 |   740
 195600 | 10100
 198300 |   300
 194700 |   856
 195200 |  2100
 197400 |  3460
 195300 |  2000
 198700 |  4275
 195900 |   720
 198200 |   400
 197900 |  1500
 197700 |   300
 198800 |   400
 196700 |   200
 195500 |  3550
 198100 |  3325
 197100 | 11100
 196000 |  2850
 196500 |   800
 194100 |   177
 196200 |   200
 197200 |  3100
 197000 | 11050
 194900 |  1000
 199500 |  2040
 197800 |   100
 195800 |   525
 195700 |   700
 197600 |  1700
 195000 | 30908
 198000 |  2300
 197300 |  4600
 199000 |  2900
 194000 | 13117
 198600 |   677
 199400 |    23
 198500 |  7528
 196100 |   700
(48 rows)
 */

CREATE TABLE bids(broker_id float, price float, volume float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

-- b1.price is an example of both upward and sideways propagation
SELECT b1.price, sum(b1.volume) 
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
       > (select sum(b2.volume) from bids b2 where b2.price > b1.price)
GROUP BY b1.price;
