TRUNCATE TABLE SERVER;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    rackid    integer,
    total     float
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
            SELECT s1.rackid, sum(1)
            FROM   SERVER s1
            WHERE  (SELECT sum(s2.load) FROM SERVER s2) * 1.5
                   < (SELECT sum(1) FROM SERVER s3) * s1.load
            GROUP BY s1.rackid
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_server AFTER INSERT OR DELETE ON SERVER
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_server ON SERVER;
