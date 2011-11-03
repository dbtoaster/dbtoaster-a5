TRUNCATE TABLE CUSTOMER;
TRUNCATE TABLE LINEITEM;
TRUNCATE TABLE NATION;
TRUNCATE TABLE ORDERS;
TRUNCATE TABLE PART;
TRUNCATE TABLE SUPPLIER;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    sn_regionkey integer,
    cn_regionkey integer,
    type         integer,
    total        float
);

CREATE OR REPLACE FUNCTION recompute_query() RETURNS TRIGGER AS $recompute_query$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        TRUNCATE TABLE RESULTS;
        INSERT INTO RESULTS (
            SELECT sn.regionkey, 
                   cn.regionkey,
                   PART.type,
                   SUM(LINEITEM.quantity)
            FROM   CUSTOMER, ORDERS, LINEITEM, PART, SUPPLIER, NATION cn, NATION sn
            WHERE  CUSTOMER.custkey = ORDERS.custkey
              AND  ORDERS.orderkey = LINEITEM.orderkey
              AND  PART.partkey = LINEITEM.partkey
              AND  SUPPLIER.suppkey = LINEITEM.suppkey
              AND  ORDERS.orderdate >= 19970101
              AND  ORDERS.orderdate <  19980101
              AND  cn.nationkey = CUSTOMER.nationkey
              AND  sn.nationkey = SUPPLIER.nationkey
            GROUP BY sn.regionkey, cn.regionkey, PART.type
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

CREATE TRIGGER refresh_lineitem AFTER INSERT OR DELETE ON PART
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
DROP TRIGGER refresh_lineitem ON PART;
DROP TRIGGER refresh_orders ON SUPPLIER;
