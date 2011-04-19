create table SSB_DATE (datekey bigint, year text);

create table SSB_CUSTOMER (
        custkey bigint,
        name text, address text, city text,
        nation text, region text,
        phone text, mktsegment text
        );

create table SSB_SUPPLIER (
        suppkey bigint,
        name text, address text, city text,
        nation text, region text, phone text
        );

create table SSB_PART (
        partkey bigint,
        name text, mfgr text,
        category text, brand1 text,
        color text, type text,
        size integer, container text
        );

create table SSB_LINEORDER (
        orderkey bigint,
        linenumber integer,
        custkey bigint, partkey bigint, suppkey bigint,
        orderdate bigint,
        orderpriority text, shippriority integer,
        quantity double,
        extendedprice double, ordtotalprice double,
        discount double, revenue double, supplycost double,
        tax double,
        commitdate bigint,
        shipmode text
        );

select d.year, c.nation, c.region, s.region, p.mfgr, sum(lo.revenue + ((-1) * lo.supplycost))
from
    ssb_date d,
    ssb_customer c,
    ssb_supplier s,
    ssb_part p,
    ssb_lineorder lo
where lo.custkey = c.custkey
and lo.suppkey = s.suppkey
and lo.partkey = p.partkey
and lo.orderdate = d.datekey
group by d.year, c.nation, c.region, s.region, p.mfgr;