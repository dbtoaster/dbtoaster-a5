/* Result:
   sum    
----------
 6945.61
(1 row)
 */

CREATE TABLE LINEITEM (
        orderkey       int,
        partkey        int,
        suppkey        int,
        linenumber     int,
        quantity       int,
        extendedprice  float,
        discount       float,
        tax            float,
        -- the fields below should be text, but since dbtoaster
        -- does not handle strings, we make them floats for now
        -- by hashing in the adaptor
        returnflag     int, -- hash
        linestatus     int, -- hash
        shipdate       int, -- date
        commitdate     int, -- date
        receiptdate    int, -- date
        shipinstruct   int, -- hash
        shipmode       int, -- hash
        comment        int  -- hash
    )
  FROM FILE 'test/data/tpch_tiny/lineitem.csv'
  LINE DELIMITED lineitem;

CREATE TABLE PART (
        partkey      int,
        name         int, -- hash
        mfgr         int, -- hash
        brand        int, -- hash
        type         int, -- hash
        size         int,
        container    int, -- hash
        retailprice  float,
        comment      int  -- hash
    )
  FROM FILE 'test/data/tpch_tiny/part.csv'
  LINE DELIMITED part;

select sum(l.extendedprice)
from   lineitem l, part p
where  p.partkey = l.partkey
and    l.quantity < 0.05*
       (select sum(l2.quantity)
        from lineitem l2 where l2.partkey = p.partkey);
