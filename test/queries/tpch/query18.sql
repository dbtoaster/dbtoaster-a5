INCLUDE 'test/queries/tpch/schemas.sql';

SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, 
       sum(l.quantity) AS query18
FROM customer c, orders o, lineitem l
WHERE o.orderkey IN 
  ( SELECT l3.orderkey FROM (
      SELECT l2.orderkey, SUM(l2.quantity) AS QTY 
      FROM lineitem l2 GROUP BY l2.orderkey 
    ) l3
    WHERE QTY > 100
  )
 AND c.custkey = o.custkey
 AND o.orderkey = l.orderkey
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice;


--SELECT c.custkey, sum(l1.quantity) AS query18
--FROM customer c, orders o, lineitem l1
--WHERE 1 <=
--      (SELECT sum(1) FROM lineitem l2
--       WHERE l1.orderkey = l2.orderkey
--       AND 100 < (SELECT sum(l3.quantity) FROM lineitem l3
--                  WHERE l2.orderkey = l3.orderkey))
--AND c.custkey = o.custkey
--AND o.orderkey = l1.orderkey
--GROUP BY c.custkey;
