/* Results on vwap5k:
 broker_id |   sum    
-----------+----------
         0 |  9081878
         6 | 27173838
         5 | 11926243
         1 | 23716107
         2 | 18210052
         3 | 14472798
         7 | 16616456
         9 | 10780335
         8 | 11329251
         4 | 19046934
(10 rows)
 */

CREATE TABLE bids(broker_id float, p float, v float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

CREATE TABLE asks(broker_id float, p float, v float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', validate := 'true', brokers := '10');

select b.broker_id, sum(a.v+b.v)
from bids b, asks a
where 0.25*(select sum(a1.v) from asks a1) >
           (select sum(a2.v) from asks a2 where a2.p > a.p)
and   0.25*(select sum(b1.v) from bids b1) >
           (select sum(b2.v) from bids b2 where b2.p > b.p)
group by b.broker_id;