SET search_path = 'TPCH_@@DATASET@@';

SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                    THEN l_extendedprice*(1-l_discount) ELSE 0
                    END) / 
                SUM(l_extendedprice * (1 - l_discount)) as promo_revenue 
FROM lineitem, part 
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE('1995-09-01')
  AND l_shipdate <  DATE('1995-10-01');

-- SELECT 100.00 * (local.revenue / total.revenue) AS promo_revenue 
-- FROM 
--   (
--     SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
--     FROM lineitem l, part p
--     WHERE l.l_partkey = p.p_partkey
--       AND l.l_shipdate >= DATE('1995-09-01') 
--       AND l.l_shipdate <  DATE('1995-10-01')
--       AND (p.p_type LIKE 'PROMO%')
--   ) local, 
--   (
--     SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
--     FROM lineitem l, part p
--     WHERE l.l_partkey = p.p_partkey
--       AND l.l_shipdate >= DATE('1995-09-01') 
--       AND l.l_shipdate <  DATE('1995-10-01')  
--   ) total;
