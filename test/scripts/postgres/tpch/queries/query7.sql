SET search_path = 'TPCH_@@DATASET@@';

SELECT quote_literal(supp_nation), quote_literal(cust_nation), 
       l_year, SUM(volume) as revenue
FROM (
  SELECT n1.n_name AS supp_nation,
         n2.n_name AS cust_nation,
         (DATE_PART('year', l.l_shipdate)) AS l_year,
         l.l_extendedprice * (1 - l.l_discount) AS volume
  FROM supplier s, lineitem l, orders o, customer c, nation n1, nation n2
  WHERE s.s_suppkey = l.l_suppkey
    AND o.o_orderkey = l.l_orderkey
    AND c.c_custkey = o.o_custkey
    AND s.s_nationkey = n1.n_nationkey 
    AND c.c_nationkey = n2.n_nationkey 
    AND (
      (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') 
        OR
      (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
    )
    AND (l.l_shipdate BETWEEN DATE('1995-01-01') AND DATE('1996-12-31') )
  ) AS shipping
GROUP BY supp_nation, cust_nation, l_year;
