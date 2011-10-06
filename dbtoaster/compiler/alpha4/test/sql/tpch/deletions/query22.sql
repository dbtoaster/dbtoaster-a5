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

select c1.nationkey, sum(c1.acctbal) from customer c1
where c1.acctbal <
    (select sum(c2.acctbal) from customer c2 where c2.acctbal > 0)
and 0 = (select sum(1) from orders o where o.custkey = c1.custkey)
group by c1.nationkey