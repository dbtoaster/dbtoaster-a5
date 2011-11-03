CREATE TABLE PARTSUPP (
        partkey      int,
        suppkey      int,
        availqty     int,
        supplycost   float,
        comment      int  -- hash
    )
  FROM FILE '../../experiments/data/tpch_100M/partsupp.csv'
  LINE DELIMITED partsupp;

CREATE TABLE SUPPLIER (
        suppkey      int,
        name         int, -- hash
        address      int, -- hash
        nationkey    int,
        phone        int, -- hash
        acctbal      float,
        comment      int  -- hash
    )
  FROM FILE '../../experiments/data/tpch_100M/supplier.csv'
  LINE DELIMITED supplier;

select ps.partkey, sum(ps.supplycost * ps.availqty)
from  partsupp ps, supplier s
where ps.suppkey = s.suppkey
group by ps.partkey;
