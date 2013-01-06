CREATE STREAM LINEITEM (
        orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DECIMAL,
        extendedprice  DECIMAL,
        discount       DECIMAL,
        tax            DECIMAL,
        returnflag     CHAR(1),
        linestatus     CHAR(1),
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   CHAR(25),
        shipmode       CHAR(10),
        comment        VARCHAR(44)
    )
  FROM FILE '@@DATASET@@/lineitem.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');


CREATE STREAM ORDERS (
        orderkey       INT,
        custkey        INT,
        orderstatus    CHAR(1),
        totalprice     DECIMAL,
        orderdate      DATE,
        orderpriority  CHAR(15),
        clerk          CHAR(15),
        shippriority   INT,
        comment        VARCHAR(79)
    )
  FROM FILE '@@DATASET@@/orders.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');

CREATE STREAM PART (
        partkey      INT,
        name         VARCHAR(55),
        mfgr         CHAR(25),
        brand        CHAR(10),
        type         VARCHAR(25),
        size         INT,
        container    CHAR(10),
        retailprice  DECIMAL,
        comment      VARCHAR(23)
    )
  FROM FILE '@@DATASET@@/part.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');


CREATE STREAM CUSTOMER (
        custkey      INT,
        name         VARCHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        mktsegment   CHAR(10),
        comment      VARCHAR(117)
    )
  FROM FILE '@@DATASET@@/customer.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');

CREATE STREAM SUPPLIER (
        suppkey      INT,
        name         CHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        comment      VARCHAR(101)
    )
  FROM FILE '@@DATASET@@/supplier.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');

CREATE STREAM PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '@@DATASET@@/partsupp.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');

CREATE TABLE NATION (
        nationkey    INT,
        name         CHAR(25),
        regionkey    INT,
        comment      VARCHAR(152)
    )
  FROM FILE '@@DATASET@@/nation.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');

CREATE TABLE REGION (
        regionkey    INT,
        name         CHAR(25),
        comment      VARCHAR(152)
    )
  FROM FILE '@@DATASET@@/region.csv'
  LINE DELIMITED CSV (delimiter := '|', deletions := '@@DELETIONS@@');


SELECT 1
FROM lineitem;  