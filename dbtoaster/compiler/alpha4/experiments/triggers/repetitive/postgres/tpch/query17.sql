DROP TABLE IF EXISTS LINEITEM;
CREATE TABLE LINEITEM (LIKE LINEITEM_STREAM);

DROP TABLE IF EXISTS PART;
CREATE TABLE PART (LIKE PART_STREAM);

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (total double precision);

CREATE OR REPLACE FUNCTION on_insert_lineitemf() RETURNS TRIGGER AS $on_insert_lineitemf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
          SELECT
            case when
                sum(l.extendedprice) is null then 0
                else sum(l.extendedprice)
            end
          FROM  lineitem l, part p
          WHERE p.partkey = l.partkey
          AND   l.quantity < 0.005 * (
            SELECT
              case when
                  sum(l2.quantity) is null then 0
                  else sum(l2.quantity)
              end
            FROM lineitem l2
            WHERE l2.partkey = p.partkey
          )
        LOOP
            UPDATE RESULTS SET total = total + item.total;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_lineitemf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_partf() RETURNS TRIGGER AS $on_insert_partf$
    DECLARE
        item RESULTS%ROWTYPE;
    BEGIN
        FOR item IN
          SELECT
            case when
                sum(l.extendedprice) is null then 0
                else sum(l.extendedprice)
            end
          FROM  lineitem l, part p
          WHERE p.partkey = l.partkey
          AND   l.quantity < 0.005 * (
            SELECT
              case when
                  sum(l2.quantity) is null then 0
                  else sum(l2.quantity)
              end
            FROM lineitem l2
            WHERE l2.partkey = p.partkey
          )
        LOOP
            UPDATE RESULTS SET total = total + item.total;

            IF found THEN
                CONTINUE;
            END IF;

            INSERT INTO RESULTS VALUES (item.total);
        END LOOP;

        RETURN new;
    END;
$on_insert_partf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_lineitem AFTER INSERT ON LINEITEM
    FOR EACH ROW EXECUTE PROCEDURE on_insert_lineitemf();

CREATE TRIGGER on_insert_part AFTER INSERT ON PART
    FOR EACH ROW EXECUTE PROCEDURE on_insert_partf();

INSERT INTO LINEITEM SELECT * FROM LINEITEM_STREAM;
INSERT INTO PART     SELECT * FROM PART_STREAM;

SELECT * FROM RESULTS;

DROP TRIGGER on_insert_lineitem ON LINEITEM;
DROP TRIGGER on_insert_part     ON PART;

DROP TABLE RESULTS;

DROP TABLE LINEITEM;
DROP TABLE PART;
