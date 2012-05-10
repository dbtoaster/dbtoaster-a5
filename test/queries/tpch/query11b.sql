CREATE TABLE PARTSUPP (
        partkey      int,
        suppkey      int,
        availqty     int,
        supplycost   double,
        comment      double  -- text
    )
  FROM FILE '../../experiments/data/tpch/partsupp.tbl'
  LINE DELIMITED partsupp;

CREATE TABLE SUPPLIER (
        suppkey      int,
        name         double, -- text
        address      double, -- text
        nationkey    int,
        phone        double, -- text
        acctbal      double,
        comment      double  -- text
    )
  FROM FILE '../../experiments/data/tpch/supplier.tbl'
  LINE DELIMITED supplier;

select sum(ps.supplycost * ps.availqty)
from  partsupp ps, supplier s
where ps.suppkey = s.suppkey
