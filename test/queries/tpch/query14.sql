-- Unsupported features for this query
--   CASE     (using equivalent query)
--   INTERVAL (inlined into constant)

INCLUDE 'test/queries/tpch/schemas.sql';

SELECT 100.00 * (local.revenue / total.revenue) AS promo_revenue 
FROM 
  (
    SELECT SUM(l.extendedprice * (1 - l.discount)) AS revenue
    FROM lineitem l, part p
    WHERE l.partkey = p.partkey
      AND l.shipdate >= DATE('1995-09-01') 
      AND l.shipdate <  DATE('1995-10-01')
      AND (p.type LIKE 'PROMO%')
  ) local, 
  (
    SELECT SUM(l.extendedprice * (1 - l.discount)) AS revenue
    FROM lineitem l, part p
    WHERE l.partkey = p.partkey
      AND l.shipdate >= DATE('1995-09-01') 
      AND l.shipdate <  DATE('1995-10-01')  
  ) total;