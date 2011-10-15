TRUNCATE TABLE ASKS;
TRUNCATE TABLE BIDS;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    total     float
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
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
