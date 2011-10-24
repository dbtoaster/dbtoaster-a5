TRUNCATE TABLE ASKS;
TRUNCATE TABLE BIDS;

CREATE TABLE RESULTS (
    total     float
);

CREATE OR REPLACE PROCEDURE recompute_query() AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    TRUNCATE TABLE RESULTS;
    INSERT INTO RESULTS (
        SELECT
            case when sum(a.price + -1 * b.price) is NULL then 0
            else sum(a.price + -1 * b.price) end
        FROM ASKS a, BIDS b
        WHERE (b.volume > 0.0001 * (
                SELECT
                    case when sum(b1.volume) is NULL then 0
                    else sum(b1.volume) end
                FROM BIDS b1
            )
        ) AND ( a.volume > 0.0001 * (
                SELECT 
                    case when sum(a1.volume) is NULL then 0
                    else sum(a1.volume) end
                FROM ASKS a1
            )
        )
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

CREATE DIRECTORY pslog AS '/tmp';
CALL dispatch('PSLOG', 'pricespread.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY pslog;
DROP TRIGGER refresh_asks;
DROP TRIGGER refresh_bids;

exit