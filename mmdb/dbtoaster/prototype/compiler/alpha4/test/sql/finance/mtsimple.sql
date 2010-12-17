CREATE TABLE bids(broker_id float, v float, p float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

CREATE TABLE asks(broker_id float, v float, p float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', validate := 'true', brokers := '10');

select b.broker_id, sum(a.p*a.v+-1*b.p*b.v)
from  bids b, asks a
where 0.25*(select sum(b1.v) from bids b1) >
           (select sum(b2.v) from bids b2 where b2.p > b.p)
group by b.broker_id;