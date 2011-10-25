CREATE TABLE CUSTOMER (
    custkey          integer,
    name             integer,
    address          integer,
    nationkey        integer,
    phone            integer,
    acctbal          float,
    mktsegment       integer,
    comments          integer
);

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
    comments          integer
);

CREATE TABLE NATION (
    nationkey        integer,
    name             integer,
    regionkey        integer,
    comments          integer
);

CREATE TABLE ORDERS (
    orderkey         integer,
    custkey          integer,
    orderstatus      integer,
    totalprice       float,
    orderdate        integer,
    orderpriority    integer,
    clerk            integer,
    shippriority     integer,
    comments          integer
);

CREATE TABLE PARTSUPP (
    partkey          integer,
    suppkey          integer,
    availqty         integer,
    supplycost       float,
    comments          integer
);

CREATE TABLE PART (
    partkey          integer,
    name             integer,
    mfgr             integer,
    brand            integer,
    type             integer,
    p_size           integer,
    container        integer,
    retailprice      float,
    comments          integer
);

CREATE TABLE REGION (
    regionkey        integer,
    name             integer,
    comments          integer
);

CREATE TABLE SUPPLIER (
    suppkey          integer,
    name             integer,
    address          integer,
    nationkey        integer,
    phone            integer,
    acctbal          float,
    comments          integer
);

CREATE TABLE AGENDA (
    schema           character varying(20),
    event            integer,
    acctbal          float,
    address          integer,
    availqty         integer,
    brand            integer,
    clerk            integer,
    comments          integer,
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
    d_size           integer,
    suppkey          integer,
    supplycost       float,
    tax              float,
    totalprice       float,
    type             integer
);

CREATE OR REPLACE PROCEDURE dispatch(log_dir IN VARCHAR2, log_file_name IN VARCHAR2)
AS
    item AGENDA%ROWTYPE;

    tally integer;
    result_count integer;
    total_count integer;
    
    CURSOR agenda_iterator IS SELECT * FROM AGENDA;

    ts varchar2(64);
    log_file UTL_FILE.FILE_TYPE;

BEGIN
	  log_file := UTL_FILE.FOPEN(log_dir, log_file_name, 'A');
    tally := 0;
    SELECT count(*) INTO total_count FROM AGENDA;
    
    OPEN agenda_iterator;
    LOOP
        FETCH agenda_iterator INTO item;
        EXIT WHEN agenda_iterator%NOTFOUND;

        tally := tally + 1;
        EXECUTE IMMEDIATE 'SELECT count(*) FROM RESULTS' INTO result_count;

        SELECT TO_CHAR (SYSTIMESTAMP, 'MM-DD-YYYY HH24:MI:SS:FF') INTO ts FROM DUAL;
        UTL_FILE.PUT_LINE(log_file,
          ts || ',' || to_char(tally) || ','
          || to_char(total_count) || ',' || to_char(result_count));

        UTL_FILE.FFLUSH(log_file);

        case item.schema

          when 'CUSTOMER'
          then case item.event

            when 1
            then INSERT INTO CUSTOMER values
              (item.custkey, item.name, item.address, item.nationkey, item.phone, item.acctbal,
               item.mktsegment, item.comments);
            
            when 0
            then DELETE FROM CUSTOMER where custkey = item.custkey;

          end case;

          when 'LINEITEM'
          then case item.event

            when 1
            then INSERT INTO LINEITEM values
              (item.orderkey, item.partkey, item.suppkey, item.linenumber, item.quantity,
               item.extendedprice, item.discount, item.tax, item.returnflag, item.linestatus,
               item.shipdate, item.commitdate, item.receiptdate, item.shipinstruct,
               item.shipmode, item.comments);

            when 0
            then DELETE FROM LINEITEM WHERE orderkey = item.orderkey AND linenumber = item.linenumber;

          end case;

          when 'NATION'
          then case item.event 

            when 1
            then INSERT INTO NATION values (item.nationkey, item.name, item.regionkey, item.comments);

            when 0
            then DELETE FROM NATION WHERE nationkey = item.nationkey;

          end case;

          when 'ORDERS'
          then case item.event

            when 1
            then INSERT INTO ORDERS values
              (item.orderkey, item.custkey, item.orderstatus, item.totalprice, item.orderdate,
               item.orderpriority, item.clerk, item.shippriority, item.comments);
            
            when 0
            then DELETE FROM ORDERS WHERE orderkey = item.orderkey;

          end case;

          when 'PART'
          then case item.event

            when 1
            then INSERT INTO PART values
              (item.partkey, item.name, item.mfgr, item.brand, item.type, item.d_size, item.container,
               item.retailprice, item.comments);
            
            when 0
            then DELETE FROM PART WHERE partkey = item.partkey;

          end case;

          when 'REGION'
          then case item.event

            when 1
            then INSERT INTO REGION values (item.regionkey, item.name, item.comments);
            
            when 0
            then DELETE FROM REGION WHERE regionkey = item.regionkey;

          end case;

          when 'PARTSUPP'
          then case item.event

            when 1
            then INSERT INTO PARTSUPP values
              (item.partkey, item.suppkey, item.availqty, item.supplycost, item.comments);

            when 0
            then DELETE FROM PARTSUPP WHERE partkey = item.partkey AND suppkey = item.suppkey;

          end case;

          when 'SUPPLIER'
          then case item.event

            when 1
            then INSERT INTO SUPPLIER values
              (item.suppkey, item.name, item.address, item.nationkey, item.phone, item.acctbal, item.comments);

            when 0
            then DELETE FROM SUPPLIER WHERE suppkey = item.suppkey;

          end case;
        end case;
    END LOOP;

    CLOSE agenda_iterator;
    UTL_FILE.FCLOSE(log_file);
END dispatch;

/

exit