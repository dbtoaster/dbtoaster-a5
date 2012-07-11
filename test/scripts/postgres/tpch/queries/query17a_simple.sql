SET search_path = 'TPCH_@@DATASET@@';

SELECT COALESCE(SUM(l.l_extendedprice), 0) AS query17
FROM   lineitem l, 
       (SELECT p.p_partkey, SUM(l2.l_quantity) AS sum_key
        FROM lineitem l2, part p
        WHERE l2.l_partkey = p.p_partkey
        GROUP BY p.p_partkey) AS p
WHERE  p.p_partkey = l.l_partkey
  AND  l.l_quantity < 0.005 * p.sum_key;