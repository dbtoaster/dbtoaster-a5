/* Result on vwap5k:
 price  |  sum  
--------+-------
 194500 |  1300
 194600 |   285
 196900 |  1300
 192100 |   360
 195400 |  1000
 193500 |  2010
 191400 |  3200
 193000 |  3710
 196800 |  2200
 195100 |  2500
 197500 |   300
 196600 |   399
 191200 |  1000
 192800 |   700
 195600 | 10100
 192700 |   150
 194700 |   856
 192000 |  1193
 192200 |  5100
 195200 |  2100
 193100 |   100
 197400 |   100
 195300 |  2000
 192600 |  2061
 191900 |    50
 195900 |   720
 197700 |   300
 193200 |    25
 196700 |   200
 195500 |  3550
 196000 |  2850
 197100 | 11100
 196500 |   800
 191100 |  1100
 193600 |   300
 194100 |   177
 196200 |   200
 191000 | 11542
 197200 |  2800
 192500 |  2975
 197000 | 11050
 194900 |  1000
 191800 |   100
 195800 |   525
 193400 |   100
 195700 |   700
 197600 |   300
 195000 | 30908
 191500 |  1509
 197300 |  4200
 192900 |   100
 194000 | 13117
 193300 |  1500
 196100 |   700
(54 rows)
 */

CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
  FROM FILE '../../experiments/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10');

-- b1.price is an example of both upward and sideways propagation
SELECT b1.price, sum(b1.volume) 
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
       > (select sum(b2.volume) from bids b2 where b2.price > b1.price)
GROUP BY b1.price;
