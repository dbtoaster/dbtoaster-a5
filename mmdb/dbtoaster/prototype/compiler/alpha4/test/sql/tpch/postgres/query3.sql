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

-- Clean up.
DROP TABLE LINEITEM;
DROP TABLE ORDERS;
DROP TABLE CUSTOMER;