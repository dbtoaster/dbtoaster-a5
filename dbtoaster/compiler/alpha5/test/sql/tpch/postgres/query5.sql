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

DROP TABLE IF EXISTS SUPPLIER;
CREATE TABLE SUPPLIER (
        suppkey      integer,
        name         text,
        address      text,
        nationkey    integer,
        phone        text,
        acctbal      double precision,
        comment      text
    );

DROP TABLE IF EXISTS NATION;
CREATE TABLE NATION (
        nationkey    integer,
        name         text,
        regionkey    integer,
        comment      text
    );
  
DROP TABLE IF EXISTS REGION;
CREATE TABLE REGION (
        regionkey    integer,
        name         text,
        comment      text
    );

    
COPY LINEITEM
FROM '@@PATH@@/test/data/tpch/lineitem.csv' WITH DELIMITER '|';

COPY ORDERS
FROM '@@PATH@@/test/data/tpch/orders.csv' WITH DELIMITER '|';

COPY CUSTOMER
FROM '@@PATH@@/test/data/tpch/customer.csv' WITH DELIMITER '|';

COPY SUPPLIER
FROM '@@PATH@@/test/data/tpch/supplier.csv' WITH DELIMITER '|';

COPY REGION
FROM '@@PATH@@/test/data/tpch/region.csv' WITH DELIMITER '|';

COPY NATION
FROM '@@PATH@@/test/data/tpch/nation.csv' WITH DELIMITER '|';


select n.name, sum(l.extendedprice * (1 + -1*l.discount))
from customer c, orders o, lineitem l, supplier s, nation n, region r
where c.custkey = o.custkey
and l.orderkey  = o.orderkey
and l.suppkey   = s.suppkey
and c.nationkey = s.nationkey
and s.nationkey = n.nationkey
and n.regionkey = r.regionkey
group by n.name;

DROP TABLE LINEITEM;
DROP TABLE ORDERS;
DROP TABLE CUSTOMER;
DROP TABLE SUPPLIER;
DROP TABLE REGION;
DROP TABLE NATION;
