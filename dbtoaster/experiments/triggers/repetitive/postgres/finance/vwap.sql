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
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_bids AFTER INSERT OR DELETE ON BIDS
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_bids ON BIDS;
