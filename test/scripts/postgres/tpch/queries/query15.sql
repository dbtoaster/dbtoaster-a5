SET search_path = 'TPCH_@@DATASET@@';

SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, CAST(100 * r1.total_revenue AS int8)
FROM supplier s, 
     (SELECT l.l_suppkey AS supplier_no, 
             SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
      FROM lineitem l
      WHERE l.l_shipdate >= DATE('1996-01-01')
        AND l.l_shipdate <  DATE('1996-04-01')
      GROUP BY l.l_suppkey) AS r1 
WHERE 
    s.s_suppkey = r1.supplier_no
    AND (NOT EXISTS (SELECT 1
                     FROM (SELECT l.l_suppkey, 
                                  SUM(l.l_extendedprice * (1 - l.l_discount)) 
                                      AS total_revenue
                           FROM lineitem l
                           WHERE l.l_shipdate >= DATE('1996-01-01')
                             AND l.l_shipdate <  DATE('1996-04-01')
                           GROUP BY l.l_suppkey) AS r2
                     WHERE r2.total_revenue > r1.total_revenue) );

