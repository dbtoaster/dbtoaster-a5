-- The correct answer to Q5 is done by nation name.  However, for ease of
-- validation, we use the nationid instead.  Both results on the canonical 
-- dataset are shown below.
--       name      |       sum        
-- ----------------+------------------
--  FRANCE         | 26667158.8530999
--  INDIA          | 36272867.5184001
--  ROMANIA        |    27350345.8278
--  CHINA          | 44678593.9358001
--  VIETNAM        |     31834053.855
--  IRAN           |    35771030.7947
--  MOROCCO        |    32725068.8962
--  SAUDI ARABIA   |     35827832.436
--  MOZAMBIQUE     |    26366356.1375
--  BRAZIL         |    36264557.6322
--  ETHIOPIA       |    29447094.8207
--  ARGENTINA      |    30290494.7397
--  EGYPT          |    35544694.5274
--  KENYA          |    26955523.7857
--  INDONESIA      | 39286503.8203001
--  JORDAN         |     22096426.003
--  IRAQ           |    36349295.3331
--  UNITED KINGDOM |    28454682.1614
--  GERMANY        |     42265166.775
--  JAPAN          |    35727093.6313
--  CANADA         |    32340731.7701
--  ALGERIA        |    28366643.0299
--  RUSSIA         |    36508031.0309
--  UNITED STATES  | 26840654.3967001
--  PERU           |    31904733.9555
-- (25 rows)
--  nationkey |      sum      
-- -----------+---------------
--          0 | 28366643.0299
--          1 | 30290494.7397
--          2 | 36264557.6322
--          3 | 32340731.7701
--          4 | 35544694.5274
--          5 | 29447094.8207
--          6 | 26667158.8531
--          7 |  42265166.775
--          8 | 36272867.5184
--          9 | 39286503.8203
--         10 | 35771030.7947
--         11 | 36349295.3331
--         12 | 35727093.6313
--         13 |  22096426.003
--         14 | 26955523.7857
--         15 | 32725068.8962
--         16 | 26366356.1375
--         17 | 31904733.9555
--         18 | 44678593.9358
--         19 | 27350345.8278
--         20 |  35827832.436
--         21 |  31834053.855
--         22 | 36508031.0309
--         23 | 28454682.1614
--         24 | 26840654.3967
-- (25 rows)

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


CREATE TABLE SUPPLIER (
        suppkey      int,
        name         int, -- hash
        address      int, -- hash
        nationkey    int,
        phone        int, -- hash
        acctbal      double,
        comment      int -- hash
    )
  FROM FILE 'test/data/tpch/supplier.tbl'
  LINE DELIMITED supplier;

CREATE TABLE NATION (
        nationkey    int,
        name         int, -- hash
        regionkey    int,
        comment      int -- hash
    )
  FROM FILE 'test/data/tpch/nation.tbl'
  LINE DELIMITED nation;
  
CREATE TABLE REGION (
        regionkey    int,
        name         int, -- hash
        comment      int -- hash
    )
  FROM FILE 'test/data/tpch/region.tbl'
  LINE DELIMITED region;


select n.nationkey, sum(l.extendedprice * (1 + -1*l.discount))
from customer c, orders o, lineitem l, supplier s, nation n, region r
where c.custkey = o.custkey
and l.orderkey  = o.orderkey
and l.suppkey   = s.suppkey
and c.nationkey = s.nationkey
and s.nationkey = n.nationkey
and n.regionkey = r.regionkey
group by n.nationkey;