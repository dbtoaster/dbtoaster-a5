/* Result on vwap5k:
 broker_id |       sum       
-----------+-----------------
         0 | 288598871745900
         6 | 300317326740500
         5 | 286065766408800
         1 | 397536953331600
         2 | 241954978204000
         3 | 387782534452400
         7 | 284936011753400
         9 | 238175475171400
         8 | 252174906561700
         4 | 316089643704700
(10 rows)
 */

CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
  FROM FILE '../../experiments/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10');

CREATE TABLE asks(t float, id int, broker_id int, volume float, price float)
  FROM FILE '../../experiments/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10');

select b.broker_id, sum(a.price*a.volume+-1*b.price*b.volume)
from  bids b, asks a
where 0.25*(select sum(b1.volume) from bids b1) >
           (select sum(b2.volume) from bids b2 where b2.price > b.price)
group by b.broker_id;