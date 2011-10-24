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
        SELECT x.broker_id,
            case when sum(x.volume * x.price - y.volume * y.price) is null then 0
            else sum(x.volume * x.price - y.volume * y.price) end
        FROM bids x, bids y
        WHERE x.broker_id = y.broker_id AND x.t > y.t
        GROUP BY x.broker_id
    );
    COMMIT;
END;

/

CREATE OR REPLACE TRIGGER refresh_bids
    AFTER INSERT OR DELETE ON BIDS
    FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY bsplog AS '/tmp';
CALL dispatch('BSPLOG', 'brokerspread.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY bsplog;
DROP TRIGGER refresh_bids;

exit