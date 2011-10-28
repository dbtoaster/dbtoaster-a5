CREATE TABLE LINEITEM (
        orderkey       int,
        partkey        int,
        suppkey        int,
        linenumber     int,
        quantity       int,
        extendedprice  double,
        discount       double,
        tax            double,
        returnflag     int, -- text(1)
        linestatus     int, -- text(1)
        shipdate       int, -- date
        commitdate     int, -- date
        receiptdate    int, -- date
        shipinstruct   int,
        shipmode       int,
        comment        int
    )
  FROM FILE 'test/data/tpch_100M/lineitem.csv'
  LINE DELIMITED lineitem;


CREATE TABLE ORDERS (
        orderkey       int,
        custkey        int,
        orderstatus    int, -- text
        totalprice     double,
        orderdate      int, -- date
        orderpriority  int,
        clerk          int,
        shippriority   int,
        comment        int  -- text
    )
  FROM FILE 'test/data/tpch_100M/orders.csv'
  LINE DELIMITED orders;

CREATE TABLE CUSTOMER (
        custkey      int,
        name         int, -- text
        address      int, -- text
        nationkey    int,
        phone        int, -- text
        acctbal      double,
        mktsegment   int, -- text
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch_100M/customer.csv'
  LINE DELIMITED customer;

CREATE TABLE SUPPLIER (
        suppkey      int,
        name         int, -- text
        address      int, -- text
        nationkey    int,
        phone        int, -- text
        acctbal      double,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch_100M/supplier.csv'
  LINE DELIMITED supplier;

CREATE TABLE NATION (
        nationkey    int,
        name         int, -- text
        regionkey    int,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch_100M/nation.csv'
  LINE DELIMITED nation;

SELECT n1.name, n2.name, sum(l.extendedprice)
FROM   supplier s, lineitem l, orders o, customer c, nation n1, nation n2
WHERE   s.suppkey = l.suppkey
  AND  o.orderkey = l.orderkey
  AND  c.custkey = o.custkey
  AND  s.nationkey = n1.nationkey
  AND  c.nationkey = n2.nationkey
  AND  n1.name = 'FRANCE' AND n2.name = 'GERMANY'
  AND  l.shipdate >= DATE('1995-01-01')
  AND  l.shipdate <  DATE('1996-01-01')
GROUP BY n1.name, n2.name;