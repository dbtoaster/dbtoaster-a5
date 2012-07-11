SET search_path = 'TPCH_@@DATASET@@';

SELECT COALESCE(SUM(l.l_extendedprice), 0) AS query17
FROM   lineitem l, part p
WHERE  p.p_partkey = l.l_partkey
  AND  l.l_quantity < 0.005 *
         (SELECT COALESCE(SUM(l2.l_quantity), 0)
          FROM lineitem l2 
          WHERE l2.l_partkey = p.p_partkey);

