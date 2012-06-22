SET search_path = '@@DATASET@@';
-- SET search_path = 'TPCH_standard';


SELECT c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, 
       sum(l.l_quantity) AS query18
FROM customer c, orders o, lineitem l
WHERE o.o_orderkey IN 
  ( SELECT l3.l_orderkey FROM (
      SELECT l2.l_orderkey, SUM(l2.l_quantity) AS QTY 
      FROM lineitem l2 GROUP BY l2.l_orderkey 
    ) l3
    WHERE QTY > 100
  )
 AND c.c_custkey = o.o_custkey
 AND o.o_orderkey = l.l_orderkey
GROUP BY c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice;
