-- Unsupported features for this query
--   CASE     (rewritten into two equivalent, separate queries)
--   INTERVAL (inlined into constant)
--   ORDER BY (ignored)

INCLUDE 'test/queries/tpch/schemas.sql';

SELECT l.shipmode, COUNT(*) as high_line_count
FROM   orders o, lineitem l
WHERE  o.orderpriority IN LIST ('1-URGENT', '2-HIGH')
  AND  o.orderkey = l.orderkey
  AND  (l.shipmode IN LIST ('MAIL', 'SHIP'))
  AND  l.commitdate < l.receiptdate
  AND  l.shipdate < l.commitdate
  AND  l.receiptdate >= DATE('1994-01-01')
  AND  l.receiptdate < DATE('1995-01-01')
GROUP BY l.shipmode;

SELECT l.shipmode, COUNT(*) as high_line_count
FROM   orders o, lineitem l
WHERE  o.orderpriority NOT IN LIST ('1-URGENT', '2-HIGH')
  AND  o.orderkey = l.orderkey
  AND  (l.shipmode IN LIST ('MAIL', 'SHIP'))
  AND  l.commitdate < l.receiptdate
  AND  l.shipdate < l.commitdate
  AND  l.receiptdate >= DATE('1994-01-01')
  AND  l.receiptdate < DATE('1995-01-01')
GROUP BY l.shipmode;
