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
  FROM FILE 'test/data/tpch_w_del/lineitem.tbl'
  LINE DELIMITED lineitem ( deletions := 'true' );

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
  FROM FILE 'test/data/tpch_w_del/orders.tbl'
  LINE DELIMITED orders ( deletions := 'true' );

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
  FROM FILE 'test/data/tpch_w_del/customer.tbl'
  LINE DELIMITED customer ( deletions := 'true' );

select c.custkey, sum(l1.quantity)
from customer c, orders o, lineitem l1
where 1 <=
      (select sum(1) from lineitem l2
       where l1.orderkey = l2.orderkey
       and 100 < (select sum(l3.quantity) from lineitem l3
                  where l2.orderkey = l3.orderkey))
and c.custkey = o.custkey
and o.orderkey = l1.orderkey
group by c.custkey;