/* Result on vwap5k: 
      sum       
----------------
 76452380068302
(1 row)

result on vwap100: 389562600
 */

CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10');

CREATE TABLE asks(t float, id int, broker_id int, volume float, price float)
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10');


-- look at spread between significant orders
select sum(a.price+-1*b.price) from bids b, asks a
where ( b.volume>0.0001*(select sum(b1.volume) from bids b1) )
and ( a.volume>0.0001*(select sum(a1.volume) from asks a1) );
