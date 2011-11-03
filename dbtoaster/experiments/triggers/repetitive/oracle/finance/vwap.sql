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
    total     float
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
        SELECT
          case when sum(b1.price * b1.volume) is NULL then 0
          else sum(b1.price * b1.volume) end
        FROM   bids b1
        WHERE  0.25 * (
            SELECT 
              case when sum(b3.volume) is NULL then 0
              else sum(b3.volume) end
            FROM bids b3
        ) > (
            SELECT 
              case when sum(b2.volume) is NULL then 0
              else sum(b2.volume) end
            FROM bids b2
            WHERE b2.price > b1.price
        )
    );
    COMMIT;
END;

/

CREATE OR REPLACE TRIGGER refresh_bids
    AFTER INSERT OR DELETE ON BIDS
    FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY vwaplog AS '/tmp';
CALL dispatch('VWAPLOG', 'vwap.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY vwaplog;
DROP TRIGGER refresh_bids;

exit