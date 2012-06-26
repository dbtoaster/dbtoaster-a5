SET search_path = '@@DATASET@@';

SELECT  cntrycode,
        COUNT(*) AS numcust,
        SUM(custsale.c_acctbal) AS totalacctbal
FROM (
  SELECT SUBSTRING(c.c_phone from 1 for 2) AS cntrycode, c.c_acctbal
  FROM   customer c
  WHERE  (SUBSTRING(c.c_phone from 1 for 2) IN
              ('13', '31', '23', '29', '30', '18', '17'))
    AND  c.c_acctbal > (
            SELECT AVG(c2.c_acctbal)
            FROM   customer c2
            WHERE  c2.c_acctbal > 0.00
            AND    (SUBSTRING(c2.c_phone from 1 for 2) IN 
                        ('13', '31', '23', '29', '30', '18', '17')))
    AND  (NOT EXISTS (SELECT * FROM orders o WHERE o.o_custkey = c.c_custkey))
  ) as custsale
GROUP BY cntrycode
