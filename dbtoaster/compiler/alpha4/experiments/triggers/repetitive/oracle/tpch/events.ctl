load data
 infile '@@PATH@@/@@AGENDA@@'
 into table agenda
 fields terminated by ","      
 ( schema, event,
   acctbal, address, availqty,
   brand, clerk, comments, commitdate, container, custkey,
   discount, extendedprice,
   linenumber, linestatus, mfgr, mktsegment,
   name, nationkey, orderdate, orderkey, orderpriority, orderstatus,
   partkey, phone, quantity,
   receiptdate, regionkey, retailprice, returnflag,
   shipdate, shipinstruct, shipmode, shippriority,
   d_size, suppkey, supplycost,
   tax, totalprice, type terminated by whitespace )