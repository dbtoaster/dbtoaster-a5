SET search_path = '@@DATASET@@';

SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, R1.total_revenue 
FROM supplier s, 
     (SELECT l.l_suppkey AS supplier_no, SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
      FROM lineitem l
      WHERE l.l_shipdate >= DATE('1996-01-01')
      AND l.l_shipdate < DATE('1996-04-01')
      GROUP BY l.l_suppkey) AS R1 
WHERE 
    s.s_suppkey = R1.supplier_no
    AND (NOT EXISTS (SELECT 1
                     FROM (SELECT l.l_suppkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
                           FROM lineitem l
                           WHERE l.l_shipdate >= DATE('1996-01-01')
                           AND l.l_shipdate < DATE('1996-04-01')
                           GROUP BY l.l_suppkey) AS R2
                     WHERE R2.total_revenue > R1.total_revenue) );

