CREATE STREAM PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '../../experiments/data/tpch/normal/partsupp.csv'
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
  FROM FILE '../../experiments/data/tpch/normal/supplier.csv'
  LINE DELIMITED CSV (fields := '|');

SELECT SUM(ps.supplycost * ps.availqty) AS query11b
FROM  partsupp ps, supplier s
WHERE ps.suppkey = s.suppkey
