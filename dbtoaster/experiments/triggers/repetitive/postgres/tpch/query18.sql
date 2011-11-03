TRUNCATE TABLE CUSTOMER;
TRUNCATE TABLE LINEITEM;
TRUNCATE TABLE ORDERS;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    custkey integer,
    total   float 
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
            SELECT c.custkey,
                case when
                    sum(l1.quantity) is null then 0
                    else sum(l1.quantity)
                end
            FROM   CUSTOMER C, LINEITEM l1, ORDERS o
            WHERE  1 <= (
                SELECT SUM(1)
                FROM LINEITEM l2
                WHERE l1.orderkey = l2.orderkey
                AND 100 < (
                    SELECT case when
                        sum(l3.quantity) is null then 0
                        else sum(l3.quantity)
                    end
                    FROM LINEITEM l3
                    WHERE l2.orderkey = l3.orderkey
                )
            )
            AND c.custkey = o.custkey
            AND o.orderkey = l1.orderkey
            GROUP BY c.custkey
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_customer AFTER INSERT OR DELETE ON CUSTOMER
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_lineitem AFTER INSERT OR DELETE ON LINEITEM
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_orders AFTER INSERT OR DELETE ON ORDERS
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_customer ON CUSTOMER;
DROP TRIGGER refresh_lineitem ON LINEITEM;
DROP TRIGGER refresh_orders ON ORDERS;
