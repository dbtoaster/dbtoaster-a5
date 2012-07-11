SET search_path = 'FINANCE_@@DATASET@@';

SELECT SUM(b1.price * b1.volume) AS vwap
FROM   bids b1
WHERE  0.25 * (SELECT SUM(b3.volume) FROM bids b3)
            >
       (SELECT CASE WHEN vol IS null THEN 0 ELSE vol END
        FROM (SELECT SUM(b2.volume) AS vol 
              FROM bids b2 where b2.price > b1.price) AS r);

