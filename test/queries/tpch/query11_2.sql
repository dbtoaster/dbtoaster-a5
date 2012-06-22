CREATE STREAM PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '../../experiments/data/tpch/standard/partsupp.csv'
  LINE DELIMITED CSV (fields := '|', deletions := 'false');

CREATE STREAM SUPPLIER (
        suppkey      INT,
        name         CHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        comment      VARCHAR(199)
    )
  FROM FILE '../../experiments/data/tpch/standard/supplier.csv'
  LINE DELIMITED CSV (fields := '|', deletions := 'false');

SELECT p.nationkey, p.partkey, SUM(p.value) AS QUERY11
FROM
  (
    SELECT s.nationkey, ps.partkey, sum(ps.supplycost * ps.availqty) AS value
    FROM partsupp ps, (SELECT * FROM supplier) s
    WHERE ps.suppkey = s.suppkey
    GROUP BY ps.partkey, s.nationkey
  ) p  
WHERE p.value > 0.001 * (SELECT sum(ps.supplycost * ps.availqty)
                         FROM  (SELECT * FROM partsupp) ps, 
                                supplier s
                         WHERE ps.suppkey = s.suppkey AND
                               p.nationkey = s.nationkey)
GROUP BY p.nationkey, p.partkey;
