DROP TABLE IF EXISTS InsertBids;
CREATE TABLE InsertBids(t float, order_id float, broker_id float, volume float, price float);

DROP TABLE IF EXISTS DeleteBids;
CREATE TABLE DeleteBids(t float, order_id float, broker_id float, volume float, price float);


COPY InsertBids FROM '@@PATH@@/testdata/InsertBIDS.dbtdat' WITH DELIMITER ',';
COPY DeleteBids FROM '@@PATH@@/testdata/DeleteBIDS.dbtdat' WITH DELIMITER ',';

SELECT price, volume INTO TABLE Bids FROM InsertBids
EXCEPT ALL SELECT price, volume FROM DeleteBids;

SELECT sum(b1.price * b1.volume) 
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
       > (select case when v is null then 0 else v end
          from (select sum(b2.volume) as v
                from bids b2 where b2.price > b1.price) as R);

DROP TABLE InsertBids;
DROP TABLE DeleteBids;
DROP TABLE Bids;
