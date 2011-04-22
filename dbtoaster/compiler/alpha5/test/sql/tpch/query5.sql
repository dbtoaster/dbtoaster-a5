-- TODO: it would be nice to have an @include directive, so that we do
-- not need to keep redefining TPCH tables.

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


select n.name, sum(l.extendedprice * (1 + -1*l.discount))
from customer c, orders o, lineitem l, supplier s, nation n, region r
where c.custkey = o.custkey
and l.orderkey  = o.orderkey
and l.suppkey   = s.suppkey
and c.nationkey = s.nationkey
and s.nationkey = n.nationkey
and n.regionkey = r.regionkey
group by n.name