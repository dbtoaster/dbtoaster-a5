SET search_path = 'TPCH_@@DATASET@@';

SELECT c.c_custkey, SUM(l1.l_quantity) AS query18
FROM customer c, orders o, lineitem l1
WHERE 1 <=
      (SELECT COALESCE(SUM(1), 0)
       FROM lineitem l2
       WHERE l1.l_orderkey = l2.l_orderkey
       AND 100 < (SELECT COALESCE(SUM(l3.l_quantity), 0) 
                  FROM lineitem l3
                  WHERE l2.l_orderkey = l3.l_orderkey))
AND c.c_custkey = o.o_custkey
AND o.o_orderkey = l1.l_orderkey
GROUP BY c.c_custkey;

