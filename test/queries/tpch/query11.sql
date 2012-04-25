CREATE STREAM PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '../../experiments/data/tpch/partsupp.csv'
  LINE DELIMITED csv ( deletions := 'true' );

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
  LINE DELIMITED csv ( deletions := 'true' );

SELECT p.nationkey, p.partkey, SUM(p.value)
FROM
  (
    SELECT s.nationkey, ps.partkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey
    GROUP BY ps.partkey, s.nationkey
  ) p,
  (
    SELECT s.nationkey, sum(ps.supplycost * ps.availqty) AS value
    FROM  partsupp ps, supplier s
    WHERE ps.suppkey = s.suppkey
    GROUP BY s.nationkey
  ) n
WHERE p.nationkey = n.nationkey
  AND p.value > 0.001 * n.value
GROUP BY p.nationkey, p.partkey;