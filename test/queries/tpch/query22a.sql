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

SELECT c1.nationkey, sum(c1.acctbal) AS query22
FROM customer c1
WHERE c1.acctbal <
    (SELECT sum(c2.acctbal) FROM customer c2 WHERE c2.acctbal > 0)
AND 0 = (SELECT sum(1) FROM orders o WHERE o.custkey = c1.custkey)
GROUP BY c1.nationkey
