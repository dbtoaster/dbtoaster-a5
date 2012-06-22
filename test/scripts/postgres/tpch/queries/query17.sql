SET search_path = '@@DATASET@@';
-- SET search_path = 'TPCH_standard';

SELECT sum(l.l_extendedprice) AS query17
FROM   lineitem l, part p
WHERE  p.p_partkey = l.l_partkey
AND    l.l_quantity < 0.005 *
       (SELECT sum(l2.l_quantity)
        FROM lineitem l2 WHERE l2.l_partkey = p.p_partkey);

