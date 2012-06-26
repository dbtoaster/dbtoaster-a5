SET search_path = '@@DATASET@@';

SELECT c_count, COUNT(*) AS QUERY13
FROM (  
   SELECT c.c_custkey AS c_custkey, COUNT(o.o_orderkey) AS c_count
   FROM customer c, orders o
   WHERE c.c_custkey = o.o_custkey 
     AND (o.o_comment NOT LIKE '%special%requests%')
   GROUP BY c.c_custkey
) c_orders
GROUP BY c_count;
