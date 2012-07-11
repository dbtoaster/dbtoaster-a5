SET search_path = 'TPCH_@@DATASET@@';

SELECT 100.00 * COALESCE(SUM(CASE WHEN p_type LIKE 'PROMO%'
                             THEN l_extendedprice*(1-l_discount) ELSE 0
                             END), 0) / 
                COALESCE(SUM(l_extendedprice * (1 - l_discount)), 1) 
                AS promo_revenue 
FROM lineitem, part 
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE('1995-09-01')
  AND l_shipdate <  DATE('1995-10-01');
