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
    nationkey integer,
    total   float 
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
        SELECT n.nationkey,
            case when sum(l.extendedprice * (1 + -1*l.discount)) is null then 0
            else sum(l.extendedprice * (1 + -1*l.discount)) end
        FROM CUSTOMER c, LINEITEM l, NATION n, ORDERS o, SUPPLIER s, REGION r
        WHERE
            c.custkey = o.custkey
        AND l.orderkey = o.orderkey
        AND l.suppkey = s.suppkey
        AND c.nationkey = s.nationkey
        AND s.nationkey = n.nationkey
        AND n.regionkey = r.regionkey
        GROUP BY n.nationkey
    );
    COMMIT;
END;

/

CREATE TRIGGER refresh_lineitem
  AFTER INSERT OR DELETE ON LINEITEM FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_orders
  AFTER INSERT OR DELETE ON ORDERS FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_customer
  AFTER INSERT OR DELETE ON CUSTOMER FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_supplier
  AFTER INSERT OR DELETE ON SUPPLIER FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_nation
  AFTER INSERT OR DELETE ON NATION FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_region
  AFTER INSERT OR DELETE ON REGION FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY q5log AS '/tmp';
CALL dispatch('Q5LOG', 'query5.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q5log;
DROP TRIGGER refresh_lineitem;
DROP TRIGGER refresh_orders;
DROP TRIGGER refresh_customer;
DROP TRIGGER refresh_supplier;
DROP TRIGGER refresh_nation;
DROP TRIGGER refresh_region;

exit
