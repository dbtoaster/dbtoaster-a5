SET search_path = 'TPCH_@@DATASET@@';

SELECT o.o_orderpriority, COUNT(*) AS order_count
FROM   orders o
WHERE  o.o_orderdate >= DATE('1993-07-01')
  AND  o.o_orderdate <  DATE('1993-10-01')
  AND (EXISTS (
    SELECT * FROM lineitem l
    WHERE l.l_orderkey = o.o_orderkey
      AND l.l_commitdate < l.l_receiptdate
  ))
GROUP BY o.o_orderpriority;

