SET search_path = '@@DATASET@@';

SELECT 100.00 * (local.revenue / total.revenue) AS promo_revenue 
FROM 
  (
    SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
    FROM lineitem l, part p
    WHERE l.l_partkey = p.p_partkey
      AND l.l_shipdate >= DATE('1995-09-01') 
      AND l.l_shipdate <  DATE('1995-10-01')
      AND (p.p_type LIKE 'PROMO%')
  ) local, 
  (
    SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
    FROM lineitem l, part p
    WHERE l.l_partkey = p.p_partkey
      AND l.l_shipdate >= DATE('1995-09-01') 
      AND l.l_shipdate <  DATE('1995-10-01')  
  ) total;
