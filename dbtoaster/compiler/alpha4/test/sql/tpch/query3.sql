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



CREATE TABLE LINEITEM (
        orderkey       int,
        partkey        int,
        suppkey        int,
        linenumber     int,
        quantity       int,
        extendedprice  float,
        discount       float,
        tax            float,
        -- the fields below should be text, but since dbtoaster
        -- does not handle strings, we make them floats for now
        -- by hashing in the adaptor
        returnflag     int, -- hash
        linestatus     int, -- hash
        shipdate       int, -- date
        commitdate     int, -- date
        receiptdate    int, -- date
        shipinstruct   int, -- hash
        shipmode       int, -- hash
        comment        int  -- hash
    )
  FROM FILE 'test/data/tpch/lineitem.tbl'
  LINE DELIMITED lineitem;

CREATE TABLE ORDERS (
        orderkey       int,
        custkey        int,
        orderstatus    int, -- hash
        totalprice     float,
        orderdate      int, -- date
        orderpriority  int, -- hash
        clerk          int, -- hash
        shippriority   int,
        comment        int  -- hash
    )
  FROM FILE 'test/data/tpch/orders.tbl'
  LINE DELIMITED orders;

CREATE TABLE CUSTOMER (
        custkey      int,
        name         int, -- hash
        address      int, -- hash
        nationkey    int,
        phone        int, -- hash
        acctbal      float,
        mktsegment   int, -- hash
        comment      int  -- hash
    )
  FROM FILE 'test/data/tpch/customer.tbl'
  LINE DELIMITED customer;

SELECT ORDERS.orderkey, 
       ORDERS.orderdate,
       ORDERS.shippriority,
       SUM(extendedprice * (1 - discount))
FROM   CUSTOMER, ORDERS, LINEITEM
WHERE  CUSTOMER.mktsegment = 'BUILDING'
  AND  ORDERS.custkey = CUSTOMER.custkey
  AND  LINEITEM.orderkey = ORDERS.orderkey
  AND  ORDERS.orderdate < DATE('1995-03-15')
  AND  LINEITEM.SHIPDATE > DATE('1995-03-15')
GROUP BY ORDERS.orderkey, ORDERS.orderdate, ORDERS.shippriority;