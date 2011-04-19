/* Result on vwap5k:
 broker_id |       sum       
-----------+-----------------
         0 | 240756767089200
         6 | 334203955471600
         5 | 325088206598600
         1 | 419792732208000
         2 | 335983039198600
         3 | 239692527643200
         7 | 312053101271800
         9 | 252552310059200
         8 | 252439935717800
         4 | 335810004297400
(10 rows)
 */

CREATE TABLE bids(broker_id float, p float, v float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

CREATE TABLE asks(broker_id float, p float, v float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', validate := 'true', brokers := '10');

select b.broker_id, sum(a.p*a.v+-1*b.p*b.v)
from bids b, asks a
where 0.25*(select sum(a1.v) from asks a1) >
           (select sum(a2.v) from asks a2 where a2.p > a.p)
and   0.25*(select sum(b1.v) from bids b1) >
           (select sum(b2.v) from bids b2 where b2.p > b.p)
group by b.broker_id;