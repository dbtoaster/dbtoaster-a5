drop table if exists lineitem;
drop table if exists orders;
drop table if exists part;
drop table if exists customer;
drop table if exists supplier;
drop table if exists region;
drop table if exists nation;


create table lineitem (
	orderkey bigint, partkey bigint, suppkey bigint,
	linenumber integer,
	quantity decimal, extendedprice decimal,
	discount decimal, tax decimal,
	returnflag char(1), linestatus char(1),
	shipdate date, commitdate date, receiptdate date,
	shipinstruct char(25), shipmode char(10),
	comment varchar(44),
	primary key (orderkey, linenumber));

create table orders (
	orderkey bigint, custkey bigint,
	orderstatus char(1), totalprice decimal,
	orderdate date, orderpriority char(15),
	clerk char(15), shippriority integer,
	comment varchar(79),
	primary key (orderkey));

create table part (
	partkey bigint, name varchar(55),
	mfgr char(25), brand char(10),
	type varchar(25), size integer,
	container char(10), retailprice decimal,
	comment varchar(25),
	primary key (partkey));

create table customer (
	custkey bigint, name varchar(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	mktsegment char(10), comment varchar(117),
	primary key (custkey));

create table supplier (
	suppkey bigint, name char(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	comment varchar(101),
	primary key (suppkey));

create table region (
	regionkey bigint, name char(25), comment varchar(125),
	primary key (regionkey));

create table nation (
	nationkey bigint, name char(25),
	regionkey bigint, comment varchar(152),
	primary key (nationkey));