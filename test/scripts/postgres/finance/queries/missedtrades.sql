SET search_path = 'FINANCE_@@DATASET@@';

SELECT b.broker_id, sum((a.price * a.volume) + (-1 * b.price * b.volume)) AS mst
FROM bids b, asks a
WHERE 0.25*(SELECT sum(a1.volume) FROM asks a1) >
           (SELECT CASE WHEN vol IS null THEN 0 ELSE vol END 
            FROM (SELECT sum(a2.volume) AS vol FROM asks a2 WHERE a2.price > a.price) AS r1)
AND   0.25*(SELECT sum(b1.volume) FROM bids b1) >
           (SELECT CASE WHEN vol IS null THEN 0 ELSE vol END
            FROM (SELECT sum(b2.volume) AS vol FROM bids b2 WHERE b2.price > b.price) AS r2)
GROUP BY b.broker_id;
