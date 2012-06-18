------------ Correct answer on test inputs -------------
-- orderkey  orderdate  shippriority  sum
-- 1637      1995-02-08 0             164224.9253
-- 4423      1995-02-17 0             3055.9364999999998
-- 3430      1994-12-12 0             4726.6774999999998
-- 3492      1994-11-24 0             43716.072400000005
-- 5191      1994-12-11 0             49378.309400000006
-- 742       1994-12-23 0             43728.048000000003
-- 2883      1995-01-23 0             36666.961199999998
-- 998       1994-11-26 0             11785.548600000002
--
-- or:
-- ./tpch_q3 | sed 's/; [[]/;![/g' | tr '!' '\n' | grep -v '>0.;'
-- [ 0.; 19941212.; 3430. ]->4726.6775;
-- [ 0.; 19950217.; 4423. ]->3055.9365;
-- [ 0.; 19950123.; 2883. ]->36666.9612;
-- [ 0.; 19941124.; 3492. ]->43716.0724;
-- [ 0.; 19941126.; 998. ]->11785.5486;
-- [ 0.; 19950208.; 1637. ]->164224.9253;
-- [ 0.; 19941211.; 5191. ]->49378.3094;
-- [ 0.; 19941223.; 742. ]->43728.048;



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
  LINE DELIMITED CSV (fields := '|');

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
  LINE DELIMITED CSV (fields := '|');

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
  LINE DELIMITED CSV (fields := '|');

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