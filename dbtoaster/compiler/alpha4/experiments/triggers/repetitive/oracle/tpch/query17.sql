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
    total   float 
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
      SELECT
        case when
            sum(l.extendedprice) is null then 0
            else sum(l.extendedprice)
        end
      FROM  lineitem l, part p
      WHERE p.partkey = l.partkey
      AND   l.quantity < 0.005 * (
        SELECT
          case when
              sum(l2.quantity) is null then 0
              else sum(l2.quantity)
          end
        FROM lineitem l2
        WHERE l2.partkey = p.partkey
      )
    );
    COMMIT;
END;

/

CREATE TRIGGER refresh_lineitem
  AFTER INSERT OR DELETE ON LINEITEM FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_part
  AFTER INSERT OR DELETE ON PART FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY q17log AS '/tmp';
CALL dispatch('Q17LOG', 'query17.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q17log;
DROP TRIGGER refresh_lineitem;
DROP TRIGGER refresh_part;

exit