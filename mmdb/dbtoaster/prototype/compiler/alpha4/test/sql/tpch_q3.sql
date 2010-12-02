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
        returnflag     float,
        linestatus     float,
        shipdate       int, -- date
        commitdate     int, -- date
        receiptdate    int, -- date
        shipinstruct   float,
        shipmode       float,
        comment        int
    )
  FROM FILE 'test/data/lineitem.tbl'
  LINE DELIMITED
  CSV ( fields := '|', 
        schema := 
'int,int,int,int,int,float,float,float,hash,hash,date,date,date,hash,hash,hash'
      );

CREATE TABLE ORDERS (
        orderkey       int,
        custkey        int,
        orderstatus    int, -- text
        totalprice     float,
        orderdate      int, -- date
        orderpriority  float,
        clerk          float,
        shippriority   int,
        comment        int  -- text
    )
  FROM FILE 'test/data/orders.tbl'
  LINE DELIMITED 
  CSV ( fields := '|',
        schema := 'int,int,hash,float,date,hash,hash,int,hash' );

CREATE TABLE CUSTOMER (
        custkey      int,
        name         int, -- text
        address      int, -- text
        nationkey    int,
        phone        int, -- text
        acctbal      float,
        mktsegment   int, -- text
        comment      int  -- text
    )
  FROM FILE 'test/data/customer.tbl'
  LINE DELIMITED 
  CSV ( fields := '|',
        schema := 'int,hash,hash,int,hash,float,hash,hash' );

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