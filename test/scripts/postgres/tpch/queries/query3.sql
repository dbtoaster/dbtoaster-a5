SET search_path = 'TPCH_@@DATASET@@';
-- SET search_path = 'TPCH_standard';

SELECT ORDERS.o_orderkey, 
       ORDERS.o_orderdate,
       ORDERS.o_shippriority,
       SUM(l_extendedprice * (1 - l_discount)) AS query3
FROM   CUSTOMER, ORDERS, LINEITEM
WHERE  CUSTOMER.c_mktsegment = 'BUILDING'
  AND  ORDERS.o_custkey = CUSTOMER.c_custkey
  AND  LINEITEM.l_orderkey = ORDERS.o_orderkey
  AND  ORDERS.o_orderdate < DATE('1995-03-15')
  AND  LINEITEM.l_shipdate > DATE('1995-03-15')
GROUP BY ORDERS.o_orderkey, ORDERS.o_orderdate, ORDERS.o_shippriority;
