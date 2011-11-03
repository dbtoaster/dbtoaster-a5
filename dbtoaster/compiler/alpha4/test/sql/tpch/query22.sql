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
  FROM FILE '../../experiments/data/tpch/orders.csv'
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
  FROM FILE '../../experiments/data/tpch/customer.csv'
  LINE DELIMITED customer;

select c1.nationkey, sum(c1.acctbal) from customer c1
where c1.acctbal <
    (select sum(c2.acctbal) from customer c2 where c2.acctbal > 0)
and 0 = (select sum(1) from orders o where o.custkey = c1.custkey)
group by c1.nationkey