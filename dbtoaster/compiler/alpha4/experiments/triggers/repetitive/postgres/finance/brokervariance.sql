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
            SELECT x.broker_id,
                case when sum(x.volume * x.price * y.volume * y.price * 0.5) is null then 0
                else sum(x.volume * x.price * y.volume * y.price * 0.5) end
            FROM bids x, bids y
            WHERE x.broker_id = y.broker_id
            GROUP BY x.broker_id
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_bids AFTER INSERT OR DELETE ON BIDS
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_bids ON BIDS;
