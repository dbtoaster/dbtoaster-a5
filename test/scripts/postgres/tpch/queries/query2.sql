SET search_path = '@@DATASET@@';

SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, 
       s.s_comment
FROM part p, supplier s, partsupp ps, nation n, region r
WHERE p.p_partkey = ps.ps_partkey
  AND s.s_suppkey = ps.ps_suppkey
  AND p.p_size = 15
  AND (p.p_type LIKE '%BRASS')
  AND s.s_nationkey = n.n_nationkey 
  AND n.n_regionkey = r.r_regionkey 
  AND r.r_name = 'EUROPE'
  AND (NOT EXISTS (SELECT 1
                   FROM partsupp ps2, supplier s2, nation n2, region r2
                   WHERE p.p_partkey = ps2.ps_partkey
                     AND s2.s_suppkey = ps2.ps_suppkey
                     AND s2.s_nationkey = n2.n_nationkey
                     AND n2.n_regionkey = r2.r_regionkey
                     AND r2.r_name = 'EUROPE'
                     AND ps2.ps_supplycost < ps.ps_supplycost));

