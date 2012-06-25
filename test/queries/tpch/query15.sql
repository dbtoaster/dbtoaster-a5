-- Unsupported features for this query
--   CREATE VIEW (replaced with nested query)
--   ORDER BY (ignored)

INCLUDE 'test/queries/tpch/schemas.sql';


SELECT s.suppkey, s.name, s.address, s.phone, R1.total_revenue 
FROM supplier s, 
     (SELECT l.suppkey AS supplier_no, SUM(l.extendedprice * (1 - l.discount)) AS total_revenue
      FROM lineitem l
      WHERE l.shipdate >= DATE('1996-01-01')
      AND l.shipdate < DATE('1996-04-01')
      GROUP BY l.suppkey) AS R1 
WHERE 
    s.suppkey = R1.supplier_no
    AND (NOT EXISTS (SELECT 1
                     FROM (SELECT l.suppkey, SUM(l.extendedprice * (1 - l.discount)) AS total_revenue
                            FROM lineitem l
                            WHERE l.shipdate >= DATE('1996-01-01')
                            AND l.shipdate < DATE('1996-04-03')
                            GROUP BY l.suppkey) AS R2
                     WHERE R2.total_revenue > R1.total_revenue) );
