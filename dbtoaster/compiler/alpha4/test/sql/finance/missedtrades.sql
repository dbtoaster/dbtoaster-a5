/* Result on vwap5k (with deterministic := 'yes'):

 broker_id | sum
-----------+-----------------
         0 | 31882050828000
         1 | 37469290313600
         2 | 59202436936800
         3 | 42596931596400
         4 | 42042754366800
         5 | 38393684872600
         6 | 29268645483200
         7 | 46276059514400
         8 | 47002090406800
         9 | 23587687676000
(10 rows)
 */

CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10',
                            deterministic := 'yes');

CREATE TABLE asks(t float, id int, broker_id int, volume float, price float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10',
                            deterministic := 'yes');

select b.broker_id, sum(a.price*a.volume+-1*b.price*b.volume)
from bids b, asks a
where 0.25*(select sum(a1.volume) from asks a1) >
           (select sum(a2.volume) from asks a2 where a2.price > a.price)
and   0.25*(select sum(b1.volume) from bids b1) >
           (select sum(b2.volume) from bids b2 where b2.price > b.price)
group by b.broker_id;