DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS (
        orderkey       integer,
        custkey        integer,
        orderstatus    text,
        totalprice     double precision,
        orderdate      date,
        orderpriority  text,
        clerk          text,
        shippriority   integer,
        comment        text
    );

DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER (
        custkey      integer,
        name         text,
        address      text,
        nationkey    integer,
        phone        text,
        acctbal      double precision,
        mktsegment   text,
        comment      text
    );

COPY ORDERS
FROM '@@PATH@@/test/data/tpch_100M/orders.csv' WITH DELIMITER '|';

COPY CUSTOMER
FROM '@@PATH@@/test/data/tpch_100M/customer.csv' WITH DELIMITER '|';

select c1.nationkey, sum(c1.acctbal) from customer c1
where c1.acctbal <
    (select sum(c2.acctbal) from customer c2 where c2.acctbal > 0)
and 0 = (select sum(1) from orders o where o.custkey = c1.custkey)
group by c1.nationkey;

DROP TABLE ORDERS;
DROP TABLE CUSTOMER;