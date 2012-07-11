SET search_path = 'TPCH_@@DATASET@@';

SELECT p.ps_partkey, SUM(p.value) AS value
FROM
  (
    SELECT ps.ps_partkey, 
           SUM(ps.ps_supplycost * ps.ps_availqty) AS value
    FROM  partsupp ps, supplier s, nation n
    WHERE ps.ps_suppkey = s.s_suppkey
      AND s.s_nationkey = n.n_nationkey
      AND n.n_name = 'GERMANY' 
    GROUP BY ps.ps_partkey
  ) p
WHERE p.value > (
        SELECT COALESCE(SUM(ps.ps_supplycost * ps.ps_availqty), 0) * 0.001 
               AS value
        FROM  partsupp ps, supplier s, nation n
        WHERE ps.ps_suppkey = s.s_suppkey
          AND s.s_nationkey = n.n_nationkey
          AND n.n_name = 'GERMANY'
  )
GROUP BY p.ps_partkey
