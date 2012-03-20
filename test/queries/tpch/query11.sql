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

SELECT ps.partkey, sum(ps.supplycost * ps.availqty) AS query11
FROM  partsupp ps, supplier s
WHERE ps.suppkey = s.suppkey
GROUP BY	 ps.partkey;
