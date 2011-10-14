TRUNCATE TABLE CUSTOMER;
TRUNCATE TABLE ORDERS;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    nationkey integer,
    total     float 
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
            SELECT c1.nationkey,
                case when
                    sum(c1.acctbal) is null then 0
                    else sum(c1.acctbal)
                end
            FROM CUSTOMER c1
            WHERE c1.acctbal < (
                SELECT
                    case when
                        sum(c2.acctbal) is null then 0
                        else sum(c2.acctbal)
                    end
                FROM  CUSTOMER c2
                WHERE c2.acctbal > 0
            )
            AND 0 = (
                SELECT
                    case when
                        sum(1) is null then 0
                        else sum(1)
                    end
            )
            GROUP BY c1.nationkey
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_customer AFTER INSERT OR DELETE ON CUSTOMER
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_orders AFTER INSERT OR DELETE ON ORDERS
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_customer ON CUSTOMER;
DROP TRIGGER refresh_orders ON ORDERS;
