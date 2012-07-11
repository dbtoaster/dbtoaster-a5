SET search_path = 'TPCH_@@DATASET@@';

SELECT ps.ps_partkey, 
       SUM(ps.ps_supplycost * ps.ps_availqty) AS query11a
FROM  partsupp ps, supplier s
WHERE ps.ps_suppkey = s.s_suppkey
GROUP BY ps.ps_partkey;

