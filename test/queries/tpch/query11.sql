CREATE STREAM PARTSUPP (
        partkey      int,
        suppkey      int,
        availqty     int,
        supplycost   float,
        comment      int  -- hash
    )
  FROM FILE '../../experiments/data/tpch/partsupp.csv'
  LINE DELIMITED csv ( deletions := 'true' );

CREATE STREAM SUPPLIER (
        suppkey      int,
        name         int, -- hash
        address      int, -- hash
        nationkey    int,
        phone        int, -- hash
        acctbal      float,
        comment      int  -- hash
    )
  FROM FILE '../../experiments/data/tpch/supplier.csv'
  LINE DELIMITED csv ( deletions := 'true' );

SELECT ps.partkey, sum(ps.supplycost * ps.availqty) AS query11
FROM  partsupp ps, supplier s
WHERE ps.suppkey = s.suppkey
GROUP BY	 ps.partkey;
