DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER (LIKE CUSTOMER_STREAM);

DROP TABLE IF EXISTS LINEITEM;
CREATE TABLE LINEITEM (LIKE LINEITEM_STREAM);

DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS (LIKE ORDERS_STREAM);

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    custkey     integer,
    total       integer
);

CREATE OR REPLACE FUNCTION on_insert_customerf() RETURNS TRIGGER AS $on_insert_customerf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT c.custkey, sum(l1.quantity)
            FROM   CUSTOMER C, LINEITEM l1, ORDERS o
            WHERE  1 <= (
                SELECT SUM(1)
                FROM LINEITEM l2
                WHERE l1.orderkey = l2.orderkey
                AND 100 < (
                    SELECT sum(l3.quantity)
                    FROM LINEITEM l3
                    WHERE l2.orderkey = l3.orderkey
                )
            )
            AND c.custkey = o.custkey
            AND o.orderkey = l1.orderkey
            GROUP BY c.custkey
        LOOP
            UPDATE RESULTS SET total = total + item.total where custkey = item.custkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.custkey, item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_customerf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_lineitemf() RETURNS TRIGGER AS $on_insert_lineitemf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT c.custkey, sum(l1.quantity)
            FROM   CUSTOMER C, LINEITEM l1, ORDERS o
            WHERE  1 <= (
                SELECT SUM(1)
                FROM LINEITEM l2
                WHERE l1.orderkey = l2.orderkey
                AND 100 < (
                    SELECT sum(l3.quantity)
                    FROM LINEITEM l3
                    WHERE l2.orderkey = l3.orderkey
                )
            )
            AND c.custkey = o.custkey
            AND o.orderkey = l1.orderkey
            GROUP BY c.custkey
        LOOP
            UPDATE RESULTS SET total = total + item.total where custkey = item.custkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.custkey, item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_lineitemf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_ordersf() RETURNS TRIGGER AS $on_insert_ordersf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT c.custkey, sum(l1.quantity)
            FROM   CUSTOMER C, LINEITEM l1, ORDERS o
            WHERE  1 <= (
                SELECT SUM(1)
                FROM LINEITEM l2
                WHERE l1.orderkey = l2.orderkey
                AND 100 < (
                    SELECT sum(l3.quantity)
                    FROM LINEITEM l3
                    WHERE l2.orderkey = l3.orderkey
                )
            )
            AND c.custkey = o.custkey
            AND o.orderkey = l1.orderkey
            GROUP BY c.custkey
        LOOP
            UPDATE RESULTS SET total = total + item.total where custkey = item.custkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.custkey, item.total);
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
