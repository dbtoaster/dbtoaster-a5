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
    partkey float,
    total   float 
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
      SELECT ps.partkey,
        case when sum(ps.supplycost * ps.availqty) is null then 0
        else sum(ps.supplycost * ps.availqty) end
      FROM   PARTSUPP ps, SUPPLIER s
      WHERE  ps.suppkey = s.suppkey
      GROUP  BY ps.partkey
    );
    COMMIT;
END;

/

CREATE TRIGGER refresh_partsupp
  AFTER INSERT OR DELETE ON PARTSUPP FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE TRIGGER refresh_supplier
  AFTER INSERT OR DELETE ON SUPPLIER FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY q11alog AS '/tmp';
CALL dispatch('Q11ALOG', 'query11a.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q11alog;
DROP TRIGGER refresh_partsupp;
DROP TRIGGER refresh_supplier;

exit
