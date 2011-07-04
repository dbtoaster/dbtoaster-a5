/* Result on vwap5k (with deterministic := 'yes'):
 
 broker_id |   sum    
-----------+----------
         0 |  2446668
         1 |  -648039
         2 | -5363809
         3 |   864240
         4 |  8384852
         5 |  3288320
         6 | -2605617
         7 |   243551
         8 |  1565128
         9 |   995180
(10 rows)
*/

CREATE TABLE bids(t float, id int, broker_id int, volume float, price float)
--  FROM FILE '/Users/xthemage/Desktop/testdata/InsertBIDS.dbtdat' 
--  LINE DELIMITED 
--  CSV (fields := ',', schema := 'float,float,float,float,float', eventtype := 'insert');
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', brokers := '10', 
                            deterministic := 'yes');

CREATE TABLE asks(t float, id int, broker_id int, volume float, price float)
--  FROM FILE 'ASKS.dbtdat' 
--  LINE DELIMITED 
--  CSV (fields := ',', schema := 'float,float,float,float,float', eventtype := 'insert');
  FROM FILE 'test/data/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'asks', brokers := '10', 
                            deterministic := 'yes');

--SELECT b.broker_id, sum(b.volume) FROM bids b GROUP BY b.broker_id;

SELECT   b.broker_id, sum(a.volume + -1 * b.volume)
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.price + ((-1) * b.price) > 1000) OR
           (b.price + ((-1) * a.price) > 1000) )
GROUP BY b.broker_id;
