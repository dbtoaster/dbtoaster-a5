SET search_path = 'FINANCE_@@DATASET@@';

SELECT x.broker_id, SUM(x.volume * x.price * y.volume * y.price * 0.5) AS bsv
FROM   bids x, bids y
WHERE  x.broker_id = y.broker_id
GROUP BY x.broker_id;

