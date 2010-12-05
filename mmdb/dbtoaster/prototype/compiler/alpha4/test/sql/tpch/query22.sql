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
  FROM FILE 'test/data/orders.csv'
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
  FROM FILE 'test/data/customer.csv'
  LINE DELIMITED customer;

select c1.nationkey, sum(c1.acctbal) from customer c1
where c1.acctbal <
    (select sum(c2.acctbal) from customer c2 where c2.acctbal > 0)
and 0 = (select sum(1) from orders o where o.custkey = c1.custkey)
group by c1.nationkey