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
    broker_id integer,
    total     float
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS
        SELECT b.broker_id,
            case when sum(a.volume + -1 * b.volume) is NULL then 0
            else sum(a.volume + -1 * b.volume) end
        FROM ASKS a, BIDS b
        WHERE b.broker_id = a.broker_id
          AND ((a.price + ((-1) * b.price) > 1000) OR
               (b.price + ((-1) * a.price) > 1000))
        GROUP BY b.broker_id;
    COMMIT;
END;

/

CREATE OR REPLACE TRIGGER refresh_asks
  AFTER INSERT OR DELETE ON ASKS
  FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE OR REPLACE TRIGGER refresh_bids
  AFTER INSERT OR DELETE ON BIDS
  FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY axflog AS '/tmp';
CALL dispatch('AXFLOG', 'axfinder.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY axflog;
DROP TRIGGER refresh_asks;
DROP TRIGGER refresh_bids;

exit