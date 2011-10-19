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
  FROM FILE 'test/data/tpch/lineitem.csv'
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
  FROM FILE 'test/data/tpch/orders.csv'
  LINE DELIMITED orders;

CREATE TABLE PART (
        partkey      int,
        name         int, -- text
        mfgr         int, -- text
        brand        int, -- text
        type         int, -- text
        size         int,
        container    int, -- text
        retailprice  double,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch/part.csv'
  LINE DELIMITED part;

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
  FROM FILE 'test/data/tpch/customer.csv'
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
  FROM FILE 'test/data/tpch/supplier.csv'
  LINE DELIMITED supplier;

CREATE TABLE NATION (
        nationkey    int,
        name         int, -- text
        regionkey    int,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch/nation.csv'
  LINE DELIMITED nation;


 SELECT sn.regionkey, 
        cn.regionkey,
        PART.type,
        SUM(LINEITEM.quantity)
 FROM   CUSTOMER, ORDERS, LINEITEM, PART, SUPPLIER, NATION cn, NATION sn
 WHERE  CUSTOMER.custkey = ORDERS.custkey
   AND  ORDERS.orderkey = LINEITEM.orderkey
   AND  PART.partkey = LINEITEM.partkey
   AND  SUPPLIER.suppkey = LINEITEM.suppkey
   AND  ORDERS.orderdate >= DATE('1997-01-01')
   AND  ORDERS.orderdate <  DATE('1998-01-01')
   AND  cn.nationkey = CUSTOMER.nationkey
   AND  sn.nationkey = SUPPLIER.nationkey
 GROUP BY sn.regionkey, cn.regionkey, PART.type
