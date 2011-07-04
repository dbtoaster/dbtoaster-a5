/* Result on vwap5k:
  broker_id |       sum       
-----------+-----------------
         0 | 288177572410800
         6 | 300084847416000
         5 | 287350081689600
         1 | 396477242977200
         2 | 240623582568000
         3 | 385211560580800
         7 | 286981024200800
         9 | 239388930520800
         8 | 252041333166400
         4 | 313316399432400
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