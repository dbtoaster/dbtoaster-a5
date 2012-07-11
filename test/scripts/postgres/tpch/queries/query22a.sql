SET search_path = 'TPCH_@@DATASET@@';

SELECT c1.c_nationkey, SUM(c1.c_acctbal) AS query22
FROM customer c1
WHERE c1.c_acctbal <
    (SELECT COALESCE(SUM(c2.c_acctbal), 0)
     FROM customer c2 
     WHERE c2.c_acctbal > 0)
AND 0 = (SELECT count(*) FROM orders o WHERE o.o_custkey = c1.c_custkey)
GROUP BY c1.c_nationkey

