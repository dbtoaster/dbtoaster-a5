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
    sn_regionkey integer,
    cn_regionkey integer,
    type         integer,
    total        float
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
        SELECT sn.regionkey, 
               cn.regionkey,
               PART.type,
               SUM(LINEITEM.quantity)
        FROM   CUSTOMER, ORDERS, LINEITEM, PART, SUPPLIER, NATION cn, NATION sn
        WHERE  CUSTOMER.custkey = ORDERS.custkey
          AND  ORDERS.orderkey = LINEITEM.orderkey
          AND  PART.partkey = LINEITEM.partkey
          AND  SUPPLIER.suppkey = LINEITEM.suppkey
          AND  ORDERS.orderdate >= 19970101
          AND  ORDERS.orderdate <  19980101
          AND  cn.nationkey = CUSTOMER.nationkey
          AND  sn.nationkey = SUPPLIER.nationkey
        GROUP BY sn.regionkey, cn.regionkey, PART.type
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

CREATE TRIGGER refresh_part
  AFTER INSERT OR DELETE ON PART FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_nation
  AFTER INSERT OR DELETE ON NATION FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY ssb4log AS '/tmp';
CALL dispatch('SSB4LOG', 'ssb4.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY ssb4log;
DROP TRIGGER refresh_lineitem;
DROP TRIGGER refresh_orders;
DROP TRIGGER refresh_customer;
DROP TRIGGER refresh_supplier;
DROP TRIGGER refresh_part;
DROP TRIGGER refresh_nation;

exit
