CREATE TABLE LINEITEM (
        orderkey       int,
        partkey        int,
        suppkey        int,
        linenumber     int,
        quantity       double,
        extendedprice  double,
        discount       double,
        tax            double,
        -- the fields below should be text, but since dbtoaster
        -- does not handle strings, we make them doubles for now
        -- by hashing in the adaptor
        returnflag     double,
        linestatus     double,
        shipdate       double, -- date
        commitdate     double, -- date
        receiptdate    double, -- date
        shipinstruct   double,
        shipmode       double,
        comment        double
    )
  FROM FILE '../../experiments/data/lineitem.csv'
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
  FROM FILE '../../experiments/data/orders.csv'
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
  FROM FILE '../../experiments/data/part.csv'
  LINE DELIMITED part;

CREATE TABLE PARTSUPP (
        partkey      int,
        suppkey      int,
        availqty     int,
        supplycost   double,
        comment      double  -- text
    )
  FROM FILE '../../experiments/data/partsupp.csv'
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
  FROM FILE '../../experiments/data/customer.csv'
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
  FROM FILE '../../experiments/data/supplier.csv'
  LINE DELIMITED supplier;

SELECT sum(totalprice) from orders;