SET search_path = '@@DATASET@@';

SELECT l.l_shipmode, COUNT(*) as high_line_count
FROM   orders o, lineitem l
WHERE  o.o_orderpriority NOT IN ('1-URGENT', '2-HIGH')
  AND  o.o_orderkey = l.l_orderkey
  AND  (l.l_shipmode IN ('MAIL', 'SHIP'))
  AND  l.l_commitdate < l.l_receiptdate
  AND  l.l_shipdate < l.l_commitdate
  AND  l.l_receiptdate >= DATE('1994-01-01')
  AND  l.l_receiptdate < DATE('1995-01-01')
GROUP BY l.l_shipmode;

