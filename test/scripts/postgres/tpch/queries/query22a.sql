SET search_path = 'TPCH_@@DATASET@@';
-- SET search_path = 'TPCH_standard';


SELECT c1.c_nationkey, sum(c1.c_acctbal) AS query22
FROM customer c1
WHERE c1.c_acctbal <
    (SELECT sum(c2.c_acctbal) FROM customer c2 WHERE c2.c_acctbal > 0)
AND 0 = (SELECT count(*) FROM orders o WHERE o.o_custkey = c1.c_custkey)
GROUP BY c1.c_nationkey

