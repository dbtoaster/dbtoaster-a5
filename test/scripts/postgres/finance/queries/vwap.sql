SET search_path = 'FINANCE_@@DATASET@@';

SELECT sum(b1.price * b1.volume) AS vwap
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
            >
       (select sum(b2.volume) from bids b2 where b2.price > b1.price);

