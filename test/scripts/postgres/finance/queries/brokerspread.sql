SET search_path = 'FINANCE_@@DATASET@@';

SELECT x.broker_id, SUM((x.volume * x.price) - (y.volume * y.price)) AS bsp
FROM   bids x, bids y
WHERE  x.broker_id = y.broker_id AND x.t > y.t
GROUP BY x.broker_id;

