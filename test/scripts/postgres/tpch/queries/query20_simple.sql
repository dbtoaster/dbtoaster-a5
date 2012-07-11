SET search_path = 'TPCH_@@DATASET@@';

SELECT quote_literal(s.s_name), quote_literal(s.s_address), COUNT(*) 
FROM supplier s, nation n
WHERE s.s_suppkey IN 
        ( SELECT ps.ps_suppkey
          FROM partsupp ps,
               (SELECT l_partkey, l_suppkey, 
                       0.5 * COALESCE(SUM(l.l_quantity), 0) AS qty
                FROM lineitem l
                WHERE l.l_shipdate >= DATE('1994-01-01')
                  AND l.l_shipdate <  DATE('1995-01-01') 
                GROUP BY l_partkey, l_suppkey) AS r
          WHERE ps.ps_partkey = r.l_partkey
            AND ps.ps_suppkey = r.l_suppkey
            AND ps.ps_partkey IN ( SELECT p.p_partkey
                                   FROM part p
                                   WHERE p.p_name like 'forest%' )
            AND ps.ps_availqty > r.qty )
AND s.s_nationkey = n.n_nationkey
AND n.n_name = 'CANADA'
GROUP BY s.s_name, s.s_address;