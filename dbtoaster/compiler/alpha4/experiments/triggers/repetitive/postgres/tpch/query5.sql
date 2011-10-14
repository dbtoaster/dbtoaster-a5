TRUNCATE TABLE CUSTOMER;
TRUNCATE TABLE LINEITEM;
TRUNCATE TABLE NATION;
TRUNCATE TABLE ORDERS;
TRUNCATE TABLE REGION;
TRUNCATE TABLE SUPPLIER;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    nationkey integer,
    total   float 
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
            SELECT n.nationkey,
                case when sum(l.extendedprice * (1 + -1*l.discount)) is null then 0
                else sum(l.extendedprice * (1 + -1*l.discount)) end
            FROM CUSTOMER c, LINEITEM l, NATION n, ORDERS o, SUPPLIER s, REGION r
            WHERE
                c.custkey = o.custkey
            AND l.orderkey = o.orderkey
            AND l.suppkey = s.suppkey
            AND c.nationkey = s.nationkey
            AND s.nationkey = n.nationkey
            AND n.regionkey = r.regionkey
            GROUP BY n.nationkey
        );
        RETURN new;
    END;
$recompute_query$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_customer AFTER INSERT OR DELETE ON CUSTOMER
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_lineitem AFTER INSERT OR DELETE ON LINEITEM
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_customer AFTER INSERT OR DELETE ON NATION
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_orders AFTER INSERT OR DELETE ON ORDERS
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_lineitem AFTER INSERT OR DELETE ON REGION
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

CREATE TRIGGER refresh_orders AFTER INSERT OR DELETE ON SUPPLIER
    FOR EACH ROW EXECUTE PROCEDURE recompute_query();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER refresh_customer ON CUSTOMER;
DROP TRIGGER refresh_lineitem ON LINEITEM;
DROP TRIGGER refresh_customer ON NATION;
DROP TRIGGER refresh_orders ON ORDERS;
DROP TRIGGER refresh_lineitem ON REGION;
DROP TRIGGER refresh_orders ON SUPPLIER;
