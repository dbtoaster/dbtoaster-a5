CREATE STREAM LINEITEM (
        orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DECIMAL,
        extendedprice  DECIMAL,
        discount       DECIMAL,
        tax            DECIMAL,
        returnflag     CHAR(1),
        linestatus     CHAR(1),
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   CHAR(25),
        shipmode       CHAR(10),
        comment        VARCHAR(44)
    )
  FROM FILE '../../experiments/data/tpch/lineitem.csv'
  LINE DELIMITED CSV (fields := '|', schema := 'int,int,int,int,float,float,float,float,string,string,date,date,date,string,string,string', eventtype := 'insert');

CREATE STREAM ORDERS (
        orderkey       INT,
        custkey        INT,
        orderstatus    CHAR(1),
        totalprice     DECIMAL,
        orderdate      DATE,
        orderpriority  CHAR(15),
        clerk          CHAR(15),
        shippriority   INT,
        comment        VARCHAR(79)
    )
  FROM FILE '../../experiments/data/tpch/orders.csv'
  LINE DELIMITED CSV (fields := '|', schema := 'int,int,string,float,date,string,string,int,string', eventtype := 'insert');

CREATE STREAM CUSTOMER (
        custkey      INT,
        name         VARCHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        mktsegment   CHAR(10),
        comment      VARCHAR(117)
    )
  FROM FILE '../../experiments/data/tpch/customer.csv'
  LINE DELIMITED CSV (fields := '|', schema := 'int,string,string,int,string,float,string,string', eventtype := 'insert');

SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, 
       sum(l.quantity) AS query18
FROM customer c, orders o, lineitem l
WHERE o.orderkey IN 
  ( SELECT l3.orderkey FROM (
      SELECT l2.orderkey, SUM(l2.quantity) AS QTY 
      FROM lineitem l2 GROUP BY l2.orderkey 
    ) l3
    WHERE QTY > 100
  )
 AND c.custkey = o.custkey
 AND o.orderkey = l.orderkey
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice;


--SELECT c.custkey, sum(l1.quantity) AS query18
--FROM customer c, orders o, lineitem l1
--WHERE 1 <=
--      (SELECT sum(1) FROM lineitem l2
--       WHERE l1.orderkey = l2.orderkey
--       AND 100 < (SELECT sum(l3.quantity) FROM lineitem l3
--                  WHERE l2.orderkey = l3.orderkey))
--AND c.custkey = o.custkey
--AND o.orderkey = l1.orderkey
--GROUP BY c.custkey;
