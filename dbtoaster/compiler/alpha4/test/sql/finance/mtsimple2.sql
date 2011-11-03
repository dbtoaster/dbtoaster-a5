/* Results on vwap5k:
 broker_id |   sum    
-----------+----------
         0 | 13849284
         6 | 14961515
         5 | 17998572
         1 | 17788727
         2 |  9088620
         3 | 13450416
         7 | 19998284
         9 | 15431528
         8 | 12779427
         4 |  9208602
(10 rows)
 */

CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
  FROM FILE '../../experiments/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10');

CREATE TABLE asks(t float, id int, broker_id int, volume float, price float)
  FROM FILE '../../experiments/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10');

select b.broker_id, sum(a.volume+b.volume)
from bids b, asks a
where 0.25*(select sum(a1.volume) from asks a1) >
           (select sum(a2.volume) from asks a2 where a2.price > a.price)
and   0.25*(select sum(b1.volume) from bids b1) >
           (select sum(b2.volume) from bids b2 where b2.price > b.price)
group by b.broker_id;