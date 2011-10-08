-- Needs a multiplexed input over all relations in the query to produce results, since otherwise
-- there will never be any join.

DROP TABLE IF EXISTS PARTSUPP_STREAM;
CREATE TABLE PARTSUPP_STREAM (
    stage            integer,
    flag             integer,
    partkey          integer,
    suppkey          integer,
    availqty         integer,
    supplycost       double precision,
    comment          character varying (199)
);

DROP TABLE IF EXISTS SUPPLIER_STREAM;
CREATE TABLE SUPPLIER_STREAM (
    stage            integer,
    flag             integer,
    suppkey          integer,
    name             character (25),
    address          character varying (40),
    nationkey        integer,
    phone            character (15),
    acctbal          double precision,
    comment          character varying (101)
);

DROP TABLE IF EXISTS PARTSUPP;
CREATE TABLE PARTSUPP (LIKE PARTSUPP_STREAM);
ALTER TABLE PARTSUPP DROP COLUMN stage, DROP COLUMN flag;

DROP TABLE IF EXISTS SUPPLIER;
CREATE TABLE SUPPLIER (LIKE SUPPLIER_STREAM);
ALTER TABLE SUPPLIER DROP COLUMN stage, DROP COLUMN flag;

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

CREATE OR REPLACE FUNCTION on_insert_partsupp_streamf() RETURNS TRIGGER AS $on_insert_partsupp_streamf$
    BEGIN
        IF new.flag = 1 THEN
            INSERT INTO PARTSUPP VALUES (new.partkey, new.suppkey, new.availqty, new.supplycost, new.comment);
        ELSE
            DELETE FROM PARTSUPP WHERE partkey = new.partkey AND suppkey = new.suppkey;
        END IF;

        RETURN new;
    END;
$on_insert_partsupp_streamf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_supplier_streamf() RETURNS TRIGGER AS $on_insert_supplier_streamf$
    BEGIN
        IF new.flag = 1 THEN
            INSERT INTO SUPPLIER VALUES (new.suppkey, new.name, new.address, new.nationkey, new.phone, new.acctbal, new.comment);
        ELSE
            DELETE FROM SUPPLIER WHERE suppkey = new.suppkey;
        END IF;

        RETURN new;
    END;
$on_insert_supplier_streamf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_partsupp_stream AFTER INSERT ON PARTSUPP_STREAM
    FOR EACH ROW EXECUTE PROCEDURE on_insert_partsupp_streamf();

CREATE TRIGGER on_insert_supplier_stream AFTER INSERT ON SUPPLIER_STREAM
    FOR EACH ROW EXECUTE PROCEDURE on_insert_supplier_streamf();

CREATE TRIGGER on_insert_partsupp AFTER INSERT ON PARTSUPP
    FOR EACH ROW EXECUTE PROCEDURE on_insert_partsuppf();

CREATE TRIGGER on_insert_supplier AFTER INSERT ON SUPPLIER
    FOR EACH ROW EXECUTE PROCEDURE on_insert_supplierf();

CREATE TRIGGER on_delete_partsupp AFTER DELETE ON PARTSUPP
    FOR EACH ROW EXECUTE PROCEDURE on_insert_partsuppf();

CREATE TRIGGER on_delete_supplier AFTER DELETE ON SUPPLIER
    FOR EACH ROW EXECUTE PROCEDURE on_insert_supplierf();

COPY PARTSUPP_STREAM FROM '/common/dbtoaster/data/tpch/tpch_tiny_w_del/partsupp.tbl' WITH DELIMITER '|';
COPY SUPPLIER_STREAM FROM '/common/dbtoaster/data/tpch/tpch_tiny_w_del/supplier.tbl' WITH DELIMITER '|';

SELECT * FROM RESULTS;

DROP TRIGGER on_insert_partsupp ON PARTSUPP;
DROP TRIGGER on_insert_supplier ON SUPPLIER;
DROP TRIGGER on_delete_partsupp ON PARTSUPP;
DROP TRIGGER on_delete_supplier ON SUPPLIER;
DROP TRIGGER on_insert_partsupp_stream ON PARTSUPP_STREAM;
DROP TRIGGER on_insert_supplier_stream ON SUPPLIER_STREAM;

DROP TABLE RESULTS;

DROP TABLE PARTSUPP;
DROP TABLE SUPPLIER;

DROP TABLE PARTSUPP_STREAM;
DROP TABLE SUPPLIER_STREAM;
