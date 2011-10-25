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
    custkey integer,
    total   float 
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
        SELECT c.custkey,
            case when
                sum(l1.quantity) is null then 0
                else sum(l1.quantity)
            end
        FROM   CUSTOMER C, LINEITEM l1, ORDERS o
        WHERE  1 <= (
            SELECT SUM(1)
            FROM LINEITEM l2
            WHERE l1.orderkey = l2.orderkey
            AND 100 < (
                SELECT case when
                    sum(l3.quantity) is null then 0
                    else sum(l3.quantity)
                end
                FROM LINEITEM l3
                WHERE l2.orderkey = l3.orderkey
            )
        )
        AND c.custkey = o.custkey
        AND o.orderkey = l1.orderkey
        GROUP BY c.custkey
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

CREATE DIRECTORY q18log AS '/tmp';
CALL dispatch('Q18LOG', 'query18.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q18log;
DROP TRIGGER refresh_lineitem;
DROP TRIGGER refresh_orders;
DROP TRIGGER refresh_customer;

exit
