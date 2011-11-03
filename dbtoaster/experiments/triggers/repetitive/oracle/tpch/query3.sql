BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RESULTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

/

CREATE TABLE RESULTS (
    orderkey        integer,
    orderdate       integer,
    shippriority    integer,
    total           float
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
        SELECT o.orderkey, o.orderdate, o.shippriority,
            case when
                sum(extendedprice * (1 - discount)) is null then 0
                else sum(extendedprice * (1 - discount))
            end
        FROM CUSTOMER c, LINEITEM l, ORDERS o
        WHERE c.mktsegment = 1080548553
          AND o.custkey = c.custkey
          AND l.orderkey = o.orderkey
          AND o.orderdate < 19950315
          AND l.shipdate > 19950315
        GROUP BY o.orderkey, o.orderdate, o.shippriority
    );
    COMMIT;
END;

/

CREATE TRIGGER refresh_customer
  AFTER INSERT OR DELETE ON CUSTOMER
  FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_lineitem
  AFTER INSERT OR DELETE ON LINEITEM
  FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_orders
  AFTER INSERT OR DELETE ON ORDERS
  FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY q3log AS '/tmp';
CALL dispatch('Q3LOG', 'query3.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q3log;
DROP TRIGGER refresh_customer;
DROP TRIGGER refresh_lineitem;
DROP TRIGGER refresh_orders;

exit
