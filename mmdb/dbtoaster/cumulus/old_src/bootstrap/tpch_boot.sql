-- Oracle DDL file for TPC-H

DROP TABLE LINEITEM;
DROP TABLE ORDERS;
DROP TABLE PART;
DROP TABLE PARTSUPP;
DROP TABLE CUSTOMER;
DROP TABLE SUPPLIER;
DROP TABLE NATION;
DROP TABLE REGION;

CREATE TABLE LINEITEM (
        l_orderkey       int,
        l_partkey        int,
        l_suppkey        int,
        l_linenumber     int,
        l_quantity       double precision,
        l_extendedprice  double precision,
        l_discount       double precision,
        l_tax            double precision,
        l_returnflag     char(1),
        l_linestatus     char(1),
        l_shipdate       date,
        l_commitdate     date,
        l_receiptdate    date,
        l_shipinstruct   char(25),
        l_shipmode       char(10),
        l_comment        varchar(44),
        primary key      (l_orderkey,l_linenumber)
    );

CREATE TABLE ORDERS (
        o_orderkey       int,
        o_custkey        int,
        o_orderstatus    char(1),
        o_totalprice     double precision,
        o_orderdate      date,
        o_orderpriority  char(15),
        o_clerk          char(15),
        o_shippriority   integer,
        o_comment        varchar(79),
        primary key      (o_orderkey)
    );

CREATE TABLE PART (
        p_partkey        int,
        p_name           char(55),
        p_mfgr           char(25),
        p_brand          char(10),
        p_type           varchar(25),
        p_size           integer,
        p_container      char(10),
        p_retailprice    double precision,
        p_comment        varchar(23),
        primary key      (p_partkey)
    );

CREATE TABLE PARTSUPP (
        ps_partkey        int,
        ps_suppkey        int,
        ps_availqty       int,
        ps_supplycost     double precision,
        ps_comment        varchar(199),
        primary key       (ps_partkey,ps_suppkey)
    );

CREATE TABLE CUSTOMER (
        c_custkey        int,
        c_name           varchar(25),
        c_address        varchar(40),
        c_nationkey      int,
        c_phone          char(15),
        c_acctbal        double precision,
        c_mktsegment     char(10),
        c_comment        varchar(117),
        primary key      (c_custkey)
    );

CREATE TABLE SUPPLIER (
        s_suppkey        int,
        s_name           char(25),
        s_address        varchar(40),
        s_nationkey      int,
        s_phone          char(15),
        s_acctbal        double precision,
        s_comment        varchar(101),
        primary key      (s_suppkey)
    );

CREATE TABLE NATION (
        n_nationkey      int,
        n_name           char(25),
        n_regionkey      int,
        n_comment        varchar(152),
        primary key      (n_nationkey)
    );

CREATE TABLE REGION (
        r_regionkey      int,
        r_name           char(25),
        r_comment        varchar(152),
        primary key      (r_regionkey)
    );

exit;
