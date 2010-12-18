/* Result on vwap5k:
 broker_id |       sum       
-----------+-----------------
         0 | 242340312142600
         6 | 330826148677800
         5 | 327356423930300
         1 | 419563077634000
         2 | 336090961596300
         3 | 239190926029600
         7 | 312288212738900
         9 | 253740307043600
         8 | 253407759521900
         4 | 335578902287700
(10 rows)
 */

CREATE TABLE bids(broker_id float, p float, v float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

CREATE TABLE asks(broker_id float, p float, v float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', validate := 'true', brokers := '10');

select b.broker_id, sum(a.p*a.v+-1*b.p*b.v)
from  bids b, asks a
where 0.25*(select sum(b1.v) from bids b1) >
           (select sum(b2.v) from bids b2 where b2.p > b.p)
group by b.broker_id;