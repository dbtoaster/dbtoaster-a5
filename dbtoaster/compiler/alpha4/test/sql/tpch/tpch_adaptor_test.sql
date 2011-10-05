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
        shipinstruct   int, -- text
        shipmode       int, -- text
        comment        int  -- text
    )
  FROM FILE 'test/data/tpch/lineitem.tbl'
  LINE DELIMITED lineitem;

CREATE TABLE ORDERS (
        orderkey       int,
        custkey        int,
        orderstatus    int, -- text(1)
        totalprice     double,
        orderdate      int, -- date
        orderpriority  int, -- text
        clerk          int, -- text
        shippriority   int,
        comment        int  -- text
    )
  FROM FILE 'test/data/tpch/orders.tbl'
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
  FROM FILE 'test/data/tpch/part.tbl'
  LINE DELIMITED part;

CREATE TABLE PARTSUPP (
        partkey      int,
        suppkey      int,
        availqty     int,
        supplycost   double,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch/partsupp.tbl'
  LINE DELIMITED partsupp;

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
  FROM FILE 'test/data/tpch/customer.tbl'
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
  FROM FILE 'test/data/tpch/supplier.tbl'
  LINE DELIMITED supplier;

CREATE TABLE NATION (
        nationkey    int,
        name         int, -- text
        regionkey    int,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch/nation.tbl'
  LINE DELIMITED nation;
  
CREATE TABLE REGION (
        regionkey    int,
        name         int, -- text
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch/region.tbl'
  LINE DELIMITED region;
  
SELECT sum(totalprice) from orders;