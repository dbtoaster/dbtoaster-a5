-- Unsupported features for this query
--   CASE     (using equivalent query)
--   INTERVAL (inlined into constant)
-- Changes made to make the query more "incrementality friendly"
--   floating point keys are bad -> added cast_int
--   avoiding division by zero -> added listmax

INCLUDE 'test/queries/tpch/schemas.sql';

SELECT cast_int(100.00 * (local.revenue / listmax(total.revenue, 1))) 
                AS promo_revenue 
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