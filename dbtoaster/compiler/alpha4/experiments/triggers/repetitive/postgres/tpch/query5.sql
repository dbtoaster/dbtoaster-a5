DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER (LIKE CUSTOMER_STREAM);

DROP TABLE IF EXISTS LINEITEM;
CREATE TABLE LINEITEM (LIKE LINEITEM_STREAM);

DROP TABLE IF EXISTS NATION;
CREATE TABLE NATION (LIKE NATION_STREAM);

DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS (LIKE ORDERS_STREAM);

DROP TABLE IF EXISTS REGION;
CREATE TABLE REGION (LIKE REGION_STREAM);

DROP TABLE IF EXISTS SUPPLIER;
CREATE TABLE SUPPLIER (LIKE SUPPLIER_STREAM);

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    nationkey integer,
    total double precision
);

CREATE OR REPLACE FUNCTION on_insert_customerf() RETURNS TRIGGER AS $on_insert_customerf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
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
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE nationkey = item.nationkey;

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
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE nationkey = item.nationkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_lineitemf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_nationf() RETURNS TRIGGER AS $on_insert_nationf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
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
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE nationkey = item.nationkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_nationf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_ordersf() RETURNS TRIGGER AS $on_insert_ordersf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
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
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE nationkey = item.nationkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_ordersf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_regionf() RETURNS TRIGGER AS $on_insert_regionf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
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
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE nationkey = item.nationkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_regionf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_supplierf() RETURNS TRIGGER AS $on_insert_supplierf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
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
        LOOP
            UPDATE RESULTS SET total = total + item.total
            WHERE nationkey = item.nationkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.*);
        END LOOP;

        RETURN new;
    END;
$on_insert_supplierf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_customer AFTER INSERT ON CUSTOMER
    FOR EACH ROW EXECUTE PROCEDURE on_insert_customerf();

CREATE TRIGGER on_insert_lineitem AFTER INSERT ON LINEITEM
    FOR EACH ROW EXECUTE PROCEDURE on_insert_lineitemf();

CREATE TRIGGER on_insert_nation AFTER INSERT ON NATION
    FOR EACH ROW EXECUTE PROCEDURE on_insert_nationf();

CREATE TRIGGER on_insert_orders AFTER INSERT ON ORDERS
    FOR EACH ROW EXECUTE PROCEDURE on_insert_ordersf();

CREATE TRIGGER on_insert_region AFTER INSERT ON REGION
    FOR EACH ROW EXECUTE PROCEDURE on_insert_regionf();

CREATE TRIGGER on_insert_supplier AFTER INSERT ON SUPPLIER
    FOR EACH ROW EXECUTE PROCEDURE on_insert_supplierf();

INSERT INTO CUSTOMER SELECT * FROM CUSTOMER_STREAM;
INSERT INTO LINEITEM SELECT * FROM LINEITEM_STREAM;
INSERT INTO NATION SELECT * FROM NATION_STREAM;
INSERT INTO ORDERS SELECT * FROM ORDERS_STREAM;
INSERT INTO REGION SELECT * FROM REGION_STREAM;
INSERT INTO SUPPLIER SELECT * FROM SUPPLIER_STREAM;

SELECT * FROM RESULTS;

DROP TRIGGER on_insert_customer ON CUSTOMER;
DROP TRIGGER on_insert_lineitem ON LINEITEM;
DROP TRIGGER on_insert_nation ON NATION;
DROP TRIGGER on_insert_orders ON ORDERS;
DROP TRIGGER on_insert_region ON REGION;
DROP TRIGGER on_insert_supplier ON SUPPLIER;

DROP TABLE RESULTS;

DROP TABLE CUSTOMER;
DROP TABLE LINEITEM;
DROP TABLE NATION;
DROP TABLE ORDERS;
DROP TABLE REGION;
DROP TABLE SUPPLIER;
