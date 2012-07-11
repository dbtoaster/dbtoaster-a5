SET search_path = 'TPCH_@@DATASET@@';

SELECT quote_literal(s.s_name), quote_literal(s.s_address), COUNT(*) 
FROM supplier s, nation n
WHERE s.s_suppkey IN 
        ( SELECT ps.ps_suppkey
          FROM partsupp ps
          WHERE ps.ps_partkey IN ( SELECT p.p_partkey
                                   FROM part p
                                   WHERE p.p_name like 'forest%' )
          AND ps.ps_availqty > ( SELECT 0.5 * COALESCE(SUM(l.l_quantity), 0)
                                 FROM lineitem l
                                 WHERE l.l_partkey = ps.ps_partkey
                                   AND l.l_suppkey = ps.ps_suppkey
                                   AND l.l_shipdate >= DATE('1994-01-01')
                                   AND l.l_shipdate <  DATE('1995-01-01') ) )
AND s.s_nationkey = n.n_nationkey
AND n.n_name = 'CANADA'
GROUP BY s.s_name, s.s_address;