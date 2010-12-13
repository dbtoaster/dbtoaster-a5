-- Correct answer: 3663277605

CREATE TABLE bids(broker_id float, v float, p float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true', brokers := '10');

CREATE TABLE asks(broker_id float, v float, p float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', validate := 'true', brokers := '10');


-- look at spread between significant orders
select sum(a.p+((-1)*b.p)) from bids b, asks a
where ( b.v>0.0001*(select sum(b1.v) from bids b1) )
and ( a.v>0.0001*(select sum(a1.v) from asks a1) )
