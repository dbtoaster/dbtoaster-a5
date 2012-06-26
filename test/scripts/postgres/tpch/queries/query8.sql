SET search_path = '@@DATASET@@';

SELECT  total.o_year,
        (SUM(local.volume) / SUM(total.volume)) AS mkt_share
FROM
  (
    SELECT DATE_PART('year', o.o_orderdate) AS o_year,
           l.l_extendedprice * (1-l.l_discount) AS volume
    FROM   part p, supplier s, lineitem l, orders o, customer c, nation n1,
           nation n2, region r
    WHERE  p.p_partkey = l.l_partkey
      AND  s.s_suppkey = l.l_suppkey
      AND  l.l_orderkey = o.o_orderkey
      AND  o.o_custkey = c.c_custkey
      AND  c.c_nationkey = n1.n_nationkey 
      AND  n1.n_regionkey = r.r_regionkey 
      AND  r.r_name = 'AMERICA'
      AND  s.s_nationkey = n2.n_nationkey
      AND  (o.o_orderdate BETWEEN DATE('1995-01-01') AND DATE('1996-12-31'))
      AND  p.p_type = 'ECONOMY ANODIZED STEEL'
      AND  n2.n_name = 'BRAZIL'
  ) local,
  (
    SELECT DATE_PART('year', o.o_orderdate) AS o_year,
           l.l_extendedprice * (1-l.l_discount) AS volume
    FROM   part p, supplier s, lineitem l, orders o, customer c, nation n1,
           nation n2, region r
    WHERE  p.p_partkey = l.l_partkey
      AND  s.s_suppkey = l.l_suppkey
      AND  l.l_orderkey = o.o_orderkey
      AND  o.o_custkey = c.c_custkey
      AND  c.c_nationkey = n1.n_nationkey 
      AND  n1.n_regionkey = r.r_regionkey 
      AND  r.r_name = 'AMERICA'
      AND  s.s_nationkey = n2.n_nationkey
      AND  (o.o_orderdate BETWEEN DATE('1995-01-01') AND DATE('1996-12-31'))
      AND  p.p_type = 'ECONOMY ANODIZED STEEL'
  ) total
WHERE local.o_year = total.o_year
GROUP BY total.o_year;

