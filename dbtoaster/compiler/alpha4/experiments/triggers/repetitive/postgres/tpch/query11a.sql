DROP TABLE IF EXISTS PARTSUPP;
CREATE TABLE PARTSUPP (LIKE PARTSUPP_STREAM);

DROP TABLE IF EXISTS SUPPLIER;
CREATE TABLE SUPPLIER (LIKE SUPPLIER_STREAM);

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    partkey     double precision,
    total       double precision
);

CREATE OR REPLACE FUNCTION on_insert_partsuppf() RETURNS TRIGGER AS $on_insert_partsuppf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
          SELECT ps.partkey,
            case when
                sum(ps.supplycost * ps.availqty) is null then 0
                else sum(ps.supplycost * ps.availqty)
            end
          FROM   PARTSUPP ps, SUPPLIER s
          WHERE  ps.suppkey = s.suppkey
          GROUP  BY ps.partkey
        LOOP
            UPDATE RESULTS SET total = total + item.total where partkey = item.partkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.partkey, item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_partsuppf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_supplierf() RETURNS TRIGGER AS $on_insert_supplierf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
          SELECT ps.partkey,
            case when
                sum(ps.supplycost * ps.availqty) is null then 0
                else sum(ps.supplycost * ps.availqty)
            end
          FROM   PARTSUPP ps, SUPPLIER s
          WHERE  ps.suppkey = s.suppkey
          GROUP  BY ps.partkey
        LOOP
            UPDATE RESULTS SET total = total + item.total where partkey = item.partkey;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.partkey, item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_supplierf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_partsupp AFTER INSERT ON PARTSUPP
    FOR EACH ROW EXECUTE PROCEDURE on_insert_partsuppf();

CREATE TRIGGER on_insert_supplier AFTER INSERT ON SUPPLIER
    FOR EACH ROW EXECUTE PROCEDURE on_insert_supplierf();

INSERT INTO PARTSUPP SELECT * FROM PARTSUPP_STREAM;
INSERT INTO SUPPLIER SELECT * FROM SUPPLIER_STREAM;

SELECT * FROM RESULTS;

DROP TRIGGER on_insert_partsupp ON PARTSUPP;
DROP TRIGGER on_insert_supplier ON SUPPLIER;

DROP TABLE RESULTS;

DROP TABLE PARTSUPP;
DROP TABLE SUPPLIER;
