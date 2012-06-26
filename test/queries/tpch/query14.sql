-- Unsupported features for this query
--   CASE     (using equivalent query)
--   INTERVAL (inlined into constant)

--INCLUDE 'test/queries/tpch/schemas.sql';
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
  FROM FILE '../../experiments/data/tpch/tiny/lineitem.csv'
  LINE DELIMITED CSV (fields := '|');
  
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
  FROM FILE '../../experiments/data/tpch/tiny/part.csv'
  LINE DELIMITED CSV (fields := '|');

SELECT cast_int(100.00 * (local.revenue / listmax(total.revenue, 1))) 
                AS promo_revenue 
FROM 
  (
    SELECT SUM(l.extendedprice * (1 - l.discount)) AS revenue
    FROM lineitem l, part p
    WHERE l.partkey = p.partkey
      AND l.shipdate >= DATE('1995-09-01') 
      AND l.shipdate <  DATE('1995-10-01')
      AND (p.type LIKE 'PROMO%')
  ) local, 
  (
    SELECT SUM(l.extendedprice * (1 - l.discount)) AS revenue
    FROM lineitem l, part p
    WHERE l.partkey = p.partkey
      AND l.shipdate >= DATE('1995-09-01') 
      AND l.shipdate <  DATE('1995-10-01')  
  ) total;