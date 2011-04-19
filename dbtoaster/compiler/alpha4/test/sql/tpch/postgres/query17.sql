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

DROP TABLE IF EXISTS PART;
CREATE TABLE PART (
        partkey      integer,
        name         text,
        mfgr         text,
        brand        text,
        type         text,
        size         integer,
        container    text,
        retailprice  double precision,
        comment      text
    );

COPY LINEITEM
FROM '@@PATH@@/test/data/tpch/lineitem.csv' WITH DELIMITER '|';

COPY PART
FROM '@@PATH@@/test/data/tpch/part.csv' WITH DELIMITER '|';


select sum(l.extendedprice)
from   lineitem l, part p
where  p.partkey = l.partkey
and    l.quantity < 0.005*
       (select sum(l2.quantity)
        from lineitem l2 where l2.partkey = p.partkey);

DROP TABLE LINEITEM;
DROP TABLE PART;