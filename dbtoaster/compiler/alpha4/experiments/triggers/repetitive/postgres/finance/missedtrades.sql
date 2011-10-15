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
