DROP TABLE IF EXISTS LINEITEM;
CREATE TABLE LINEITEM (
        orderkey       integer,
        partkey        integer,
        suppkey        integer,
        linenumber     integer,
        quantity       integer,
        extendedprice  double precision,
        discount       double precision,
        tax            double precision,
        returnflag     text,
        linestatus     text,
        shipdate       date,
        commitdate     date,
        receiptdate    date,
        shipinstruct   text,
        shipmode       text,
        comment        text
    );

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
    
COPY LINEITEM
FROM '@@PATH@@/test/data/tpch/lineitem.csv' WITH DELIMITER '|';

COPY ORDERS
FROM '@@PATH@@/test/data/tpch/orders.csv' WITH DELIMITER '|';

COPY CUSTOMER
FROM '@@PATH@@/test/data/tpch/customer.csv' WITH DELIMITER '|';

select c.custkey, sum(l1.quantity)
from customer c, orders o, lineitem l1
where o.orderkey in (select l3.orderkey from lineitem l3
                  l3.orderkey
                  having sum(l3.quantity) > 100))
and c.custkey = o.custkey
and o.orderkey = l1.orderkey
group by c.custkey;

DROP TABLE LINEITEM;
DROP TABLE ORDERS;
DROP TABLE CUSTOMER;