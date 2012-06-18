CREATE STREAM PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '../../experiments/data/tpch/partsupp.csv'
  LINE DELIMITED CSV (fields := '|');

CREATE STREAM SUPPLIER (
        suppkey      INT,
        name         CHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '../../experiments/data/tpch/supplier.csv'
  LINE DELIMITED CSV (fields := '|');

SELECT ps.partkey, SUM(ps.supplycost * ps.availqty) AS query11a
FROM  partsupp ps, supplier s
WHERE ps.suppkey = s.suppkey
GROUP BY ps.partkey;
