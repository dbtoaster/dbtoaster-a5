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
    total     float 
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
        SELECT c1.nationkey,
            case when
                sum(c1.acctbal) is null then 0
                else sum(c1.acctbal)
            end
        FROM CUSTOMER c1
        WHERE c1.acctbal < (
            SELECT
                case when
                    sum(c2.acctbal) is null then 0
                    else sum(c2.acctbal)
                end
            FROM  CUSTOMER c2
            WHERE c2.acctbal > 0
        )
        AND 0 = (
            SELECT
                case when
                    sum(1) is null then 0
                    else sum(1)
                end
            FROM ORDERS o
            WHERE o.custkey = c1.custkey
        )
        GROUP BY c1.nationkey
    );
    COMMIT;
END;

/

CREATE TRIGGER refresh_orders
  AFTER INSERT OR DELETE ON ORDERS FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_customer
  AFTER INSERT OR DELETE ON CUSTOMER FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY q22log AS '/tmp';
CALL dispatch('Q22LOG', 'query22.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q22log;
DROP TRIGGER refresh_orders;
DROP TRIGGER refresh_customer;

exit
