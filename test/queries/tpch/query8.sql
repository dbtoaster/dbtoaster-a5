-- Unsupported features for this query
--   CASE     (using equivalent query)
--   ORDER BY (ignored)

INCLUDE 'test/queries/tpch/schemas_tiny.sql';

SELECT  total.o_year,
        (SUM(local.volume) / SUM(total.volume)) AS mkt_share
FROM
  (
    SELECT o.orderdate AS o_year,
           DATE_PART('year', o.orderdate) AS o_year,
    FROM   part p, supplier s, lineitem l, orders o, customer c, nation n1,
           nation n2, region r
    WHERE  p.partkey = l.partkey
      AND  s.suppkey = l.suppkey
      AND  l.orderkey = o.orderkey
      AND  o.custkey = c.custkey
      AND  c.nationkey = n1.nationkey 
      AND  n1.regionkey = r.regionkey 
      AND  r.name = 'AMERICA'
      AND  s.nationkey = n2.nationkey
      AND  (o.orderdate BETWEEN DATE('1995-01-01') AND DATE('1996-12-31'))
      AND  p.type = 'ECONOMY ANODIZED STEEL'
      AND  n2.name = 'BRAZIL'
  ) local,
  (
    SELECT o.orderdate AS o_year,
--           DATE_PART('year', o.orderdate) AS o_year,
           l.extendedprice * (1-l.discount) AS volume
    FROM   part p, supplier s, lineitem l, orders o, customer c, nation n1,
           nation n2, region r
    WHERE  p.partkey = l.partkey
      AND  s.suppkey = l.suppkey
      AND  l.orderkey = o.orderkey
      AND  o.custkey = c.custkey
      AND  c.nationkey = n1.nationkey 
      AND  n1.regionkey = r.regionkey 
      AND  r.name = 'AMERICA'
      AND  s.nationkey = n2.nationkey
      AND  (o.orderdate BETWEEN DATE('1995-01-01') AND DATE('1996-12-31'))
      AND  p.type = 'ECONOMY ANODIZED STEEL'
  ) total
WHERE local.o_year = total.o_year
GROUP BY total.o_year;
