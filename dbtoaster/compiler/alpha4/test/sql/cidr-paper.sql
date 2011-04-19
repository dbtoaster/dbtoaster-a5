CREATE TABLE LINEITEM (
        ordkey       int,
        extprice  double
    )
  FROM FILE 'test/data/lineitem.csv'
  LINE DELIMITED lineitem;

CREATE TABLE ORDERS (
        ordkey       int,
        custkey        int,
        sprior   int
    )
  FROM FILE 'test/data/orders.csv'
  LINE DELIMITED orders;


CREATE TABLE CUSTOMER (
        custkey      int,
        name         double, -- text
        nationkey    int,
        acctbal      double
    )
  FROM FILE 'test/data/customer.csv'
  LINE DELIMITED customer;

select l.ordkey, o.sprior, sum(l.extprice)
  from customer c, orders o, lineitem l
  where c.custkey = o.custkey and l.ordkey = o.ordkey
  group by l.ordkey, o.sprior;
