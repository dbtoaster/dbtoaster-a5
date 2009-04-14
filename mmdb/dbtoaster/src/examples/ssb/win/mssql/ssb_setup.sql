IF OBJECT_ID(N'lineitem', 'table') IS NOT NULL
	DROP TABLE lineitem;

IF OBJECT_ID(N'orders', 'table') IS NOT NULL
	DROP TABLE orders;

IF OBJECT_ID(N'part', 'table') IS NOT NULL
	DROP TABLE part;

IF OBJECT_ID(N'customer', 'table') IS NOT NULL
	DROP TABLE customer;

IF OBJECT_ID(N'supplier', 'table') IS NOT NULL
	DROP TABLE supplier;

IF OBJECT_ID(N'region', 'table') IS NOT NULL
	DROP TABLE region;

IF OBJECT_ID(N'nation', 'table') IS NOT NULL
	DROP TABLE nation;

CREATE TABLE lineitem (
	orderkey bigint, partkey bigint, suppkey bigint,
	linenumber integer,
	quantity decimal, extendedprice decimal,
	discount decimal, tax decimal,
	returnflag char(1), linestatus char(1),
	shipdate date, commitdate date, receiptdate date,
	shipinstruct char(25), shipmode char(10),
	comment varchar(44),
	primary key (orderkey, linenumber));

CREATE TABLE orders (
	orderkey bigint, custkey bigint,
	orderstatus char(1), totalprice decimal,
	orderdate date, orderpriority char(15),
	clerk char(15), shippriority integer,
	comment varchar(79),
	primary key (orderkey));

CREATE TABLE part (
	partkey bigint, name varchar(55),
	mfgr char(25), brand char(10),
	type varchar(25), size integer,
	container char(10), retailprice decimal,
	comment varchar(25),
	primary key (partkey));

CREATE TABLE customer (
	custkey bigint, name varchar(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	mktsegment char(10), comment varchar(117),
	primary key (custkey));

CREATE TABLE supplier (
	suppkey bigint, name char(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	comment varchar(101),
	primary key (suppkey));

CREATE TABLE region (
	regionkey bigint, name char(25), comment varchar(125),
	primary key (regionkey));

CREATE TABLE nation (
	nationkey bigint, name char(25),
	regionkey bigint, comment varchar(152),
	primary key (nationkey));