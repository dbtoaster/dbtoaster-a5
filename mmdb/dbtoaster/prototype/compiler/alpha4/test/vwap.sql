CREATE TABLE bids(price float, volume int);

SELECT avg(b1.price * b1.volume) 
FROM   bids b1
WHERE  0.8 * (select sum(b3.volume) from bids b3)
            >
       (select sum(b2.volume) from bids b2 where b2.price > b1.price);