SET search_path = 'FINANCE_@@DATASET@@';

SELECT   b.broker_id, SUM(a.volume + (-1 * b.volume)) AS axfinder
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.price + ((-1) * b.price) > 1000) OR
           (b.price + ((-1) * a.price) > 1000) )
GROUP BY b.broker_id;

