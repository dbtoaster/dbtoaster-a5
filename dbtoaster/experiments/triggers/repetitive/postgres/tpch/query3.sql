TRUNCATE TABLE CUSTOMER;
TRUNCATE TABLE LINEITEM;
TRUNCATE TABLE ORDERS;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    orderkey        integer,
    orderdate       integer,
    shippriority    integer,
    total           float
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
            SELECT o.orderkey, o.orderdate, o.shippriority,
                case when
                    sum(extendedprice * (1 - discount)) is null then 0
                    else sum(extendedprice * (1 - discount))
                end
            FROM CUSTOMER c, LINEITEM l, ORDERS o
            WHERE c.mktsegment = 1080548553
              AND o.custkey = c.custkey
              AND l.orderkey = o.orderkey
              AND o.orderdate < 19950315
              AND l.shipdate > 19950315
            GROUP BY o.orderkey, o.orderdate, o.shippriority
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
