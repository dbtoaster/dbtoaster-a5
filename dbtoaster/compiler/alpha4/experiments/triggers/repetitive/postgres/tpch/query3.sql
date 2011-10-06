DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER (LIKE CUSTOMER_STREAM);

DROP TABLE IF EXISTS LINEITEM;
CREATE TABLE LINEITEM (LIKE LINEITEM_STREAM);

DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS (LIKE ORDERS_STREAM);

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    orderkey        integer,
    orderdate       date,
    shippriority    integer,
    total           double precision
);

CREATE OR REPLACE FUNCTION on_insert_customerf() RETURNS TRIGGER AS $on_insert_customerf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT o.orderkey, o.orderdate, o.shippriority,
                case when
                    sum(extendedprice * (1 - discount)) is null then 0
                    else sum(extendedprice * (1 - discount))
                end
            FROM CUSTOMER c, LINEITEM l, ORDERS o
            WHERE c.mktsegment = 'BUILDING'
              AND o.custkey = c.custkey
              AND l.orderkey = o.orderkey
              AND o.orderdate < DATE('1995-03-15')
              AND l.shipdate > DATE('1995-03-15')
            GROUP BY o.orderkey, o.orderdate, o.shippriority
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE orderkey = item.orderkey
              AND orderdate = item.orderdate
              AND shippriority = item.shippriority;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_customerf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_lineitemf() RETURNS TRIGGER AS $on_insert_lineitemf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT o.orderkey, o.orderdate, o.shippriority,
                case when
                    sum(extendedprice * (1 - discount)) is null then 0
                    else sum(extendedprice * (1 - discount))
                end
            FROM CUSTOMER c, LINEITEM l, ORDERS o
            WHERE c.mktsegment = 'BUILDING'
              AND o.custkey = c.custkey
              AND l.orderkey = o.orderkey
              AND o.orderdate < DATE('1995-03-15')
              AND l.shipdate > DATE('1995-03-15')
            GROUP BY o.orderkey, o.orderdate, o.shippriority
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE orderkey = item.orderkey
              AND orderdate = item.orderdate
              AND shippriority = item.shippriority;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_lineitemf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_ordersf() RETURNS TRIGGER AS $on_insert_ordersf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT o.orderkey, o.orderdate, o.shippriority,
                case when
                    sum(extendedprice * (1 - discount)) is null then 0
                    else sum(extendedprice * (1 - discount))
                end
            FROM CUSTOMER c, LINEITEM l, ORDERS o
            WHERE c.mktsegment = 'BUILDING'
              AND o.custkey = c.custkey
              AND l.orderkey = o.orderkey
              AND o.orderdate < DATE('1995-03-15')
              AND l.shipdate > DATE('1995-03-15')
            GROUP BY o.orderkey, o.orderdate, o.shippriority
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE orderkey = item.orderkey
              AND orderdate = item.orderdate
              AND shippriority = item.shippriority;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_ordersf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_customer AFTER INSERT ON CUSTOMER
    FOR EACH ROW EXECUTE PROCEDURE on_insert_customerf();

CREATE TRIGGER on_insert_lineitem AFTER INSERT ON LINEITEM
    FOR EACH ROW EXECUTE PROCEDURE on_insert_lineitemf();

CREATE TRIGGER on_insert_orders AFTER INSERT ON ORDERS
    FOR EACH ROW EXECUTE PROCEDURE on_insert_ordersf();

INSERT INTO CUSTOMER SELECT * FROM CUSTOMER_STREAM;
INSERT INTO LINEITEM SELECT * FROM LINEITEM_STREAM;
INSERT INTO ORDERS SELECT * FROM ORDERS_STREAM;

SELECT * FROM RESULTS;

DROP TRIGGER on_insert_customer ON CUSTOMER;
DROP TRIGGER on_insert_lineitem ON LINEITEM;
DROP TRIGGER on_insert_orders ON ORDERS;

DROP TABLE RESULTS;

DROP TABLE CUSTOMER;
DROP TABLE LINEITEM;
DROP TABLE ORDERS;
