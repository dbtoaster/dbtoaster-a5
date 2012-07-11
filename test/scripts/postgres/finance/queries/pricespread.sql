SET search_path = 'FINANCE_@@DATASET@@';

SELECT sum(a.price + (-1 * b.price)) AS psp 
FROM bids b, asks a
WHERE ( b.volume > 0.0001 * (SELECT sum(b1.volume) FROM bids b1) )
  AND ( a.volume > 0.0001 * (SELECT sum(a1.volume) FROM asks a1) );

