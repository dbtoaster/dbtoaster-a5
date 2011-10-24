TRUNCATE TABLE ASKS;
TRUNCATE TABLE BIDS;

CREATE TABLE RESULTS (
    broker_id integer,
    total     float
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS (
        SELECT b.broker_id,
            case when sum(a.price * a.volume + -1 * b.price * b.volume) is NULL then 0
            else sum(a.price * a.volume + -1 * b.price * b.volume) end
        FROM ASKS a, BIDS b
        WHERE 0.25 * (
            SELECT 
              case when sum(a1.volume) is NULL then 0
              else sum(a1.volume) end
            from asks a1
        ) > (
            SELECT 
              case when sum(a2.volume) is NULL then 0
              else sum(a2.volume) end
            FROM asks a2
            WHERE a2.price > a.price
        )
        AND 0.25 * (
            SELECT 
              case when sum(b1.volume) is NULL then 0
              else sum(b1.volume) end
            FROM bids b1
        ) > (
            SELECT 
              case when sum(b2.volume) is NULL then 0
              else sum(b2.volume) end
            FROM bids b2
            WHERE b2.price > b.price
        )
        GROUP BY b.broker_id
    );
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

CREATE DIRECTORY mstlog AS '/tmp';
CALL dispatch('MSTLOG', 'missedtrades.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY mstlog;
DROP TRIGGER refresh_asks;
DROP TRIGGER refresh_bids;

exit