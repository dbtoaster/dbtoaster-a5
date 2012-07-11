SET search_path = 'TPCH_@@DATASET@@';

SELECT  c.c_custkey, quote_literal(c.c_name), 
        c.c_acctbal, quote_literal(n.n_name),
        quote_literal(c.c_address),
        quote_literal(c.c_phone),
        quote_literal(c.c_comment),
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM    customer c, orders o, lineitem l, nation n
WHERE   c.c_custkey = o.o_custkey
  AND   l.l_orderkey = o.o_orderkey
  AND   o.o_orderdate >= DATE('1993-10-01')
  AND   o.o_orderdate < DATE('1994-01-01')
  AND   l.l_returnflag = 'R'
  AND   c.c_nationkey = n.n_nationkey
GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment
