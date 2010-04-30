CREATE TABLE bids(price float, volume int)
  FROM POSTGRES dbtoaster.vwap_5(action string, price float, volume int; 
                                 events := 'B:insert,D:delete');

SELECT avg(b1.price * b1.volume) 
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
            >
       (select sum(b2.volume) from bids b2 where b2.price > b1.price);