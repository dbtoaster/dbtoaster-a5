DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER (LIKE CUSTOMER_STREAM);

DROP TABLE IF EXISTS ORDERS;
CREATE TABLE ORDERS (LIKE ORDERS_STREAM);

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    nationkey   integer,
    total       double precision
);

CREATE OR REPLACE FUNCTION on_insert_customerf() RETURNS TRIGGER AS $on_insert_customerf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT c1.nationkey, sum(c1.acctbal)
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
        LOOP
            UPDATE RESULTS SET total = total + item.total where nationkey = item.nationkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.nationkey, item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_customerf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_ordersf() RETURNS TRIGGER AS $on_insert_ordersf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
            SELECT c1.nationkey, sum(c1.acctbal)
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
                FROM ORDERS o
                WHERE o.custkey = c1.custkey
            )
            GROUP BY c1.nationkey
        LOOP
            UPDATE RESULTS SET total = total + item.total where nationkey = item.nationkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.nationkey, item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_ordersf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_customer AFTER INSERT ON CUSTOMER
    FOR EACH ROW EXECUTE PROCEDURE on_insert_customerf();

CREATE TRIGGER on_insert_orders AFTER INSERT ON ORDERS
    FOR EACH ROW EXECUTE PROCEDURE on_insert_ordersf();

INSERT INTO CUSTOMER SELECT * FROM CUSTOMER_STREAM;
INSERT INTO ORDERS SELECT * FROM ORDERS_STREAM;

SELECT * FROM RESULTS;

DROP TRIGGER on_insert_customer ON CUSTOMER;
DROP TRIGGER on_insert_orders ON ORDERS;

DROP TABLE RESULTS;

DROP TABLE CUSTOMER;
DROP TABLE ORDERS;
