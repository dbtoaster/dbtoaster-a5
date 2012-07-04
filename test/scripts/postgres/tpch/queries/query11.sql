SET search_path = 'TPCH_@@DATASET@@';

SELECT p.s_nationkey, p.ps_partkey, SUM(p.value) AS QUERY11
FROM
  (
    SELECT s.s_nationkey, ps.ps_partkey, sum(ps.ps_supplycost * ps.ps_availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.ps_suppkey = s.s_suppkey
    GROUP BY ps.ps_partkey, s.s_nationkey
  ) p,
  (
    SELECT s.s_nationkey, sum(ps.ps_supplycost * ps.ps_availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.ps_suppkey = s.s_suppkey
    GROUP BY s.s_nationkey
  ) n
WHERE p.s_nationkey = n.s_nationkey
  AND p.value > 0.001 * n.value
GROUP BY p.s_nationkey, p.ps_partkey;
