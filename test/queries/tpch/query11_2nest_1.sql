CREATE STREAM PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '../../experiments/data/tpch_tiny/partsupp.csv'
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
  FROM FILE '../../experiments/data/tpch_tiny/supplier.csv'
  LINE DELIMITED CSV (fields := '|');

SELECT p.nationkey, p.partkey, SUM(p.value) AS QUERY11
FROM
  (
    SELECT s.nationkey, ps.partkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey 
      AND (s.suppkey IN (SELECT ps2.suppkey FROM partsupp ps2 WHERE ps2.partkey = 2))
    GROUP BY ps.partkey, s.nationkey
  ) p
WHERE p.value > (
    SELECT sum(ps.supplycost * ps.availqty) * 0.001
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey AND s.nationkey = p.nationkey
  )
GROUP BY p.nationkey, p.partkey;
