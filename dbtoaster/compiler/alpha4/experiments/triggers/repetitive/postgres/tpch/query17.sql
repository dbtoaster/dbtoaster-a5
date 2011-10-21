TRUNCATE TABLE PART;
TRUNCATE TABLE LINEITEM;

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
            case when
                sum(l.extendedprice) is null then 0
                else sum(l.extendedprice)
            end
          FROM  lineitem l, part p
          WHERE p.partkey = l.partkey
          AND   l.quantity < 0.005 * (
            SELECT
              case when
                  sum(l2.quantity) is null then 0
                  else sum(l2.quantity)
              end
            FROM lineitem l2
            WHERE l2.partkey = p.partkey
          )
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_part AFTER INSERT OR DELETE ON PART
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_lineitem AFTER INSERT OR DELETE ON LINEITEM
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_part ON PART;
DROP TRIGGER refresh_lineitem ON LINEITEM;
