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
  FROM FILE 'test/data/tpch_w_del/lineitem.tbl'
  LINE DELIMITED lineitem ( deletions := 'true' );


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
  FROM FILE 'test/data/tpch_w_del/orders.tbl'
  LINE DELIMITED orders ( deletions := 'true' );

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
  FROM FILE 'test/data/tpch_w_del/part.tbl'
  LINE DELIMITED part ( deletions := 'true' );

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
  FROM FILE 'test/data/tpch_w_del/customer.tbl'
  LINE DELIMITED customer ( deletions := 'true' );

CREATE TABLE SUPPLIER (
        suppkey      int,
        name         int, -- text
        address      int, -- text
        nationkey    int,
        phone        int, -- text
        acctbal      double,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch_w_del/supplier.tbl'
  LINE DELIMITED supplier ( deletions := 'true' );

CREATE TABLE NATION (
        nationkey    int,
        name         int, -- text
        regionkey    int,
        comment      int  -- text
    )
  FROM FILE 'test/data/tpch_w_del/nation.tbl'
  LINE DELIMITED nation ( deletions := 'true' );


 SELECT sn.regionkey, 
        cn.regionkey,
        PART.type,
        SUM(LINEITEM.quantity)
 FROM   CUSTOMER, ORDERS, LINEITEM, PART, SUPPLIER, NATION cn, NATION sn
 WHERE  CUSTOMER.custkey = ORDERS.custkey
   AND  ORDERS.orderkey = LINEITEM.orderkey
   AND  PART.partkey = LINEITEM.partkey
   AND  SUPPLIER.suppkey = LINEITEM.suppkey
 -- AND  ORDERS.orderdate >= DATE('1997-01-01')
 -- AND  ORDERS.orderdate <  DATE('1998-01-01')
   AND  cn.nationkey = CUSTOMER.nationkey
   AND  sn.nationkey = SUPPLIER.nationkey
 GROUP BY sn.regionkey, cn.regionkey, PART.type
