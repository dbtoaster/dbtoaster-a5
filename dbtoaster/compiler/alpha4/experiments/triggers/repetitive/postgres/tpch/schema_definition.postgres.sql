-- Creates all tables required for TPCH queries.

DROP TABLE IF EXISTS CUSTOMER_STREAM;
CREATE TABLE CUSTOMER_STREAM (
    custkey          integer,
    name             character (25),
    address          character varying (40),
    nationkey        integer,
    phone            character (15),
    acctbal          double precision,
    mktsegment       character (15),
    comment          character varying (117)
);

DROP TABLE IF EXISTS LINEITEM_STREAM;
CREATE TABLE LINEITEM_STREAM (
    orderkey         integer,
    partkey          integer,
    suppkey          integer,
    linenumber       integer,
    quantity         double precision,
    extendedprice    double precision,
    discount         double precision,
    tax              double precision,
    returnflag       character (1),
    linestatus       character (1),
    shipdate         date,
    commitdate       date,
    receiptdate      date,
    shipinstruct     character (25),
    shipmode         character (10),
    comment          character varying (44) 
);

DROP TABLE IF EXISTS NATION_STREAM;
CREATE TABLE NATION_STREAM (
    nationkey        integer,
    name             character (25),
    regionkey        integer,
    comment          character varying (152)
);

DROP TABLE IF EXISTS ORDERS_STREAM;
CREATE TABLE ORDERS_STREAM (
    orderkey         integer,
    custkey          integer,
    orderstatus      character (1),
    totalprice       double precision,
    orderdate        date,
    orderpriority    character (15),
    clerk            character (15),
    shippriority     integer,
    comment          character varying (79)
);

DROP TABLE IF EXISTS PARTSUPP_STREAM;
CREATE TABLE PARTSUPP_STREAM (
    partkey          integer,
    suppkey          integer,
    availqty         integer,
    supplycost       double precision,
    comment          character varying (199)
);

DROP TABLE IF EXISTS PART_STREAM;
CREATE TABLE PART_STREAM (
    partkey          integer,
    name             character varying (55),
    mfgr             character (25),
    brand            character (10),
    type             character varying (25),
    size             integer,
    container        character (10),
    retailprice      double precision,
    comment          character varying (23)
);

DROP TABLE IF EXISTS REGION_STREAM;
CREATE TABLE REGION_STREAM (
    regionkey        integer,
    name             character (25),
    comment          character varying (152)
);

DROP TABLE IF EXISTS SUPPLIER_STREAM;
CREATE TABLE SUPPLIER_STREAM (
    suppkey          integer,
    name             character (25),
    address          character varying (40),
    nationkey        integer,
    phone            character (15),
    acctbal          double precision,
    comment          character varying (101)
);
