CREATE TABLE LINEITEM (
        orderkey       int,
        partkey        int,
        suppkey        int,
        linenumber     int,
        quantity       double,
        extendedprice  double,
        discount       double,
        tax            double,
        returnflag     double, -- text(1)
        linestatus     double, -- text(1)
        shipdate       double, -- date
        commitdate     double, -- date
        receiptdate    double, -- date
        shipinstruct   double,
        shipmode       double,
        comment        double
    )
  FROM FILE 'test/data/tpch/lineitem.tbl'
  LINE DELIMITED lineitem;

CREATE TABLE ORDERS (
        orderkey       int,
        custkey        int,
        orderstatus    double, -- text
        totalprice     double,
        orderdate      double, -- date
        orderpriority  double,
        clerk          double,
        shippriority   int,
        comment        double  -- text
    )
  FROM FILE 'test/data/tpch/orders.tbl'
  LINE DELIMITED orders;

CREATE TABLE PART (
        partkey      int,
        name         double, -- text
        mfgr         double, -- text
        brand        double, -- text
        type         double, -- text
        size         int,
        container    double, -- text
        retailprice  double,
        comment      double  -- text
    )
  FROM FILE 'test/data/tpch/part.tbl'
  LINE DELIMITED part;

CREATE TABLE PARTSUPP (
        partkey      int,
        suppkey      int,
        availqty     int,
        supplycost   double,
        comment      double  -- text
    )
  FROM FILE 'test/data/tpch/partsupp.tbl'
  LINE DELIMITED partsupp;

CREATE TABLE CUSTOMER (
        custkey      int,
        name         double, -- text
        address      double, -- text
        nationkey    int,
        phone        double, -- text
        acctbal      double,
        mktsegment   double, -- text
        comment      double  -- text
    )
  FROM FILE 'test/data/tpch/customer.tbl'
  LINE DELIMITED customer;

CREATE TABLE SUPPLIER (
        suppkey      int,
        name         double, -- text
        address      double, -- text
        nationkey    int,
        phone        double, -- text
        acctbal      double,
        comment      double  -- text
    )
  FROM FILE 'test/data/tpch/supplier.tbl'
  LINE DELIMITED supplier;

CREATE TABLE NATION (
        nationkey    int,
        name         double, -- text
        regionkey    int,
        comment      double  -- text
    )
  FROM FILE 'test/data/tpch/nation.tbl'
  LINE DELIMITED nation;
  
CREATE TABLE REGION (
        regionkey    int,
        name         double, -- text
        comment      double  -- text
    )
  FROM FILE 'test/data/tpch/region.tbl'
  LINE DELIMITED region;
  
SELECT sum(totalprice) from orders;