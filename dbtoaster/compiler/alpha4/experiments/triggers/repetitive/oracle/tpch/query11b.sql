TRUNCATE TABLE PARTSUPP;
TRUNCATE TABLE SUPPLIER;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    total   float 
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
          SELECT
            case when sum(ps.supplycost * ps.availqty) is null then 0
            else sum(ps.supplycost * ps.availqty) end
          FROM PARTSUPP ps, SUPPLIER s
          WHERE ps.suppkey = s.suppkey
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_partsupp AFTER INSERT OR DELETE ON PARTSUPP
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_supplier AFTER INSERT OR DELETE ON SUPPLIER
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_partsupp ON PARTSUPP;
DROP TRIGGER refresh_supplier ON SUPPLIER;
