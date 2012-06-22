SET search_path = '@@DATASET@@';
-- SET search_path = 'TPCH_standard';

 SELECT sn.n_regionkey, 
        cn.n_regionkey,
        PART.p_type,
        SUM(LINEITEM.l_quantity) AS ssb4
 FROM   CUSTOMER, ORDERS, LINEITEM, PART, SUPPLIER, NATION cn, NATION sn
 WHERE  CUSTOMER.c_custkey = ORDERS.o_custkey
   AND  ORDERS.o_orderkey = LINEITEM.l_orderkey
   AND  PART.p_partkey = LINEITEM.l_partkey
   AND  SUPPLIER.s_suppkey = LINEITEM.l_suppkey
   AND  ORDERS.o_orderdate >= DATE('1997-01-01')
   AND  ORDERS.o_orderdate <  DATE('1998-01-01')
   AND  cn.n_nationkey = CUSTOMER.c_nationkey
   AND  sn.n_nationkey = SUPPLIER.s_nationkey
 GROUP BY sn.n_regionkey, cn.n_regionkey, PART.p_type

