DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER (
    custkey          integer,
    name             integer,
    address          integer,
    nationkey        integer,
    phone            integer,
    acctbal          float,
    mktsegment       integer,
    comment          integer
);

DROP TABLE IF EXISTS LINEITEM;
CREATE TABLE LINEITEM (
    orderkey         integer,
    partkey          integer,
    suppkey          integer,
    linenumber       integer,
    quantity         float,
    extendedprice    float,
    discount         float,
    tax              float,
    returnflag       integer,
    linestatus       integer,
    shipdate         integer,
    commitdate       integer,
    receiptdate      integer,
    shipinstruct     integer,
    shipmode         integer,
    comment          integer
);

DROP TABLE IF EXISTS NATION;
CREATE TABLE NATION (
    nationkey        integer,
    name             integer,
    regionkey        integer,
    comment          integer
);

DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS (
    orderkey         integer,
    custkey          integer,
    orderstatus      integer,
    totalprice       float,
    orderdate        integer,
    orderpriority    integer,
    clerk            integer,
    shippriority     integer,
    comment          integer
);

DROP TABLE IF EXISTS PARTSUPP;
CREATE TABLE PARTSUPP (
    partkey          integer,
    suppkey          integer,
    availqty         integer,
    supplycost       float,
    comment          integer
);

DROP TABLE IF EXISTS PART;
CREATE TABLE PART (
    partkey          integer,
    name             integer,
    mfgr             integer,
    brand            integer,
    type             integer,
    size             integer,
    container        integer,
    retailprice      float,
    comment          integer
);

DROP TABLE IF EXISTS REGION;
CREATE TABLE REGION (
    regionkey        integer,
    name             integer,
    comment          integer
);

DROP TABLE IF EXISTS SUPPLIER;
CREATE TABLE SUPPLIER (
    suppkey          integer,
    name             integer,
    address          integer,
    nationkey        integer,
    phone            integer,
    acctbal          float,
    comment          integer
);

DROP TABLE IF EXISTS AGENDA;
CREATE TABLE AGENDA (
    schema           character varying(20),
    event            integer,
    acctbal          float,
    address          integer,
    availqty         integer,
    brand            integer,
    clerk            integer,
    comment          integer,
    commitdate       integer,
    container        integer,
    custkey          integer,
    discount         float,
    extendedprice    float,
    linenumber       integer,
    linestatus       integer,
    mfgr             integer,
    mktsegment       integer,
    name             integer,
    nationkey        integer,
    orderdate        integer,
    orderkey         integer,
    orderpriority    integer,
    orderstatus      integer,
    partkey          integer,
    phone            integer,
    quantity         float,
    receiptdate      integer,
    regionkey        integer,
    retailprice      float,
    returnflag       integer,
    shipdate         integer,
    shipinstruct     integer,
    shipmode         integer,
    shippriority     integer,
    size             integer,
    suppkey          integer,
    supplycost       float,
    tax              float,
    totalprice       float,
    type             integer
);

CREATE OR REPLACE FUNCTION dispatch() RETURNS void AS $dispatch$
    DECLARE
        item AGENDA%ROWTYPE;

        count integer;
        result_count integer;
        total_count integer;
    BEGIN
        count := 0;
        total_count := (SELECT count(*) FROM AGENDA);
        FOR item in SELECT * FROM AGENDA LOOP
            count := count + 1;
            result_count := (select count(*) from RESULTS);
            RAISE NOTICE 'OPERATION % %/% (%): % %', now(), count, total_count, result_count, item.schema, item.event;
            case 

              when item.schema = 'CUSTOMER'
              then case

                when item.event = 1
                then INSERT INTO CUSTOMER values
                  (item.custkey, item.name, item.address, item.nationkey, item.phone, item.acctbal,
                   item.mktsegment, item.comment);
                
                when item.event = 0
                then DELETE FROM CUSTOMER where custkey = item.custkey;

              end case;

              when item.schema = 'LINEITEM'
              then case

                when item.event = 1
                then INSERT INTO LINEITEM values
                  (item.orderkey, item.partkey, item.suppkey, item.linenumber, item.quantity,
                   item.extendedprice, item.discount, item.tax, item.returnflag, item.linestatus,
                   item.shipdate, item.commitdate, item.receiptdate, item.shipinstruct,
                   item.shipmode, item.comment);

                when item.event = 0
                then DELETE FROM LINEITEM WHERE orderkey = item.orderkey AND linenumber = item.linenumber;

              end case;

              when item.schema = 'NATION'
              then case

                when item.event = 1
                then INSERT INTO NATION values (item.nationkey, item.name, item.regionkey, item.comment);

                when item.event = 0
                then DELETE FROM NATION WHERE nationkey = item.nationkey;

              end case;

              when item.schema = 'ORDERS'
              then case

                when item.event = 1
                then INSERT INTO ORDERS values
                  (item.orderkey, item.custkey, item.orderstatus, item.totalprice, item.orderdate,
                   item.orderpriority, item.clerk, item.shippriority, item.comment);
                
                when item.event = 0
                then DELETE FROM ORDERS WHERE orderkey = item.orderkey;

              end case;

              when item.schema = 'PART'
              then case

                when item.event = 1
                then INSERT INTO PART values
                  (item.partkey, item.name, item.mfgr, item.brand, item.type, item.size, item.container,
                   item.retailprice, item.comment);
                
                when item.event = 0
                then DELETE FROM PART WHERE partkey = item.partkey;

              end case;

              when item.schema = 'REGION'
              then case

                when item.event = 1
                then INSERT INTO REGION values (item.regionkey, item.name, item.comment);
                
                when item.event = 0
                then DELETE FROM REGION WHERE regionkey = item.regionkey;

              end case;

              when item.schema = 'PARTSUPP'
              then case

                when item.event = 1
                then INSERT INTO PARTSUPP values
                  (item.partkey, item.suppkey, item.availqty, item.supplycost, item.comment);

                when item.event = 0
                then DELETE FROM PARTSUPP WHERE partkey = item.partkey AND suppkey = item.suppkey;

              end case;

              when item.schema = 'SUPPLIER'
              then case

                when item.event = 1
                then INSERT INTO SUPPLIER values
                  (item.suppkey, item.name, item.address, item.nationkey, item.phone, item.acctbal, item.comment);

                when item.event = 0
                then DELETE FROM SUPPLIER WHERE suppkey = item.suppkey;

              end case;
            end case;
        END LOOP;
    END;
$dispatch$ LANGUAGE plpgsql;
