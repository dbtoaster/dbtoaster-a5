TRUNCATE TABLE ASKS;
TRUNCATE TABLE BIDS;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    broker_id integer,
    total     float
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
            SELECT b.broker_id,
                case when sum(a.volume + -1 * b.volume) is NULL then 0
                else sum(a.volume + -1 * b.volume) end
            FROM ASKS a, BIDS b
            WHERE b.broker_id = a.broker_id
              AND ((a.price + ((-1) * b.price) > 1000) OR
                   (b.price + ((-1) * a.price) > 1000))
            GROUP BY b.broker_id
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_asks AFTER INSERT OR DELETE ON ASKS
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_bids AFTER INSERT OR DELETE ON BIDS
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_asks ON ASKS;
DROP TRIGGER refresh_bids ON BIDS;
