-- Unsupported features for this query
--   ORDER BY (ignored)
--   LIMIT    (ignored)


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
  FROM FILE '../../experiments/data/tpch/standard/lineitem.csv'
  LINE DELIMITED CSV (fields := '|', deletions := 'false');

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
  FROM FILE '../../experiments/data/tpch/standard/orders.csv'
  LINE DELIMITED CSV (fields := '|', deletions := 'false');

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
  FROM FILE '../../experiments/data/tpch/standard/customer.csv'
  LINE DELIMITED CSV (fields := '|', deletions := 'false');

SELECT ORDERS.orderkey, 
       ORDERS.orderdate,
       ORDERS.shippriority,
       SUM(extendedprice * (1 - discount)) AS query3
FROM   CUSTOMER, ORDERS, LINEITEM
WHERE  CUSTOMER.mktsegment = 'BUILDING'
  AND  ORDERS.custkey = CUSTOMER.custkey
  AND  LINEITEM.orderkey = ORDERS.orderkey
  AND  ORDERS.orderdate < DATE('1995-03-15')
  AND  LINEITEM.shipdate > DATE('1995-03-15')
GROUP BY ORDERS.orderkey, ORDERS.orderdate, ORDERS.shippriority;
