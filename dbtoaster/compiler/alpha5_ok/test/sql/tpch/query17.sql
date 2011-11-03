CREATE TABLE LINEITEM (
        orderkey       int,
        partkey        int,
        suppkey        int,
        linenumber     int,
        quantity       double,
        extendedprice  double,
        discount       double,
        tax            double,
        returnflag     double, -- text(1)
        linestatus     double, -- text(1)
        shipdate       double, -- date
        commitdate     double, -- date
        receiptdate    double, -- date
        shipinstruct   double,
        shipmode       double,
        comment        double
    )
  FROM FILE 'test/data/tpch/lineitem.tbl'
  LINE DELIMITED lineitem;

CREATE TABLE PART (
        partkey      int,
        name         double, -- text
        mfgr         double, -- text
        brand        double, -- text
        type         double, -- text
        size         int,
        container    double, -- text
        retailprice  double,
        comment      double  -- text
    )
  FROM FILE 'test/data/tpch/part.tbl'
  LINE DELIMITED part;

select sum(l.extendedprice)
from   lineitem l, part p
where  p.partkey = l.partkey
and    l.quantity < 0.005*
       (select sum(l2.quantity)
        from lineitem l2 where l2.partkey = p.partkey);
