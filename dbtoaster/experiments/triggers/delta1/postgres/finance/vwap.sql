TRUNCATE TABLE ASKS;
TRUNCATE TABLE BIDS;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    total float
);

INSERT INTO RESULTS VALUES (0);

CREATE OR REPLACE FUNCTION on_insert_bidsf() RETURNS TRIGGER AS $on_insert_bidsf$

    BEGIN

        UPDATE RESULTS SET total = total +
            (SELECT new.volume*new.price
            WHERE (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > new.price) < 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS))

          + (SELECT new.volume*new.price
            WHERE ((SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > new.price) < 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) + 0.25 * new.volume)
            AND 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) <= (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > new.price))

          - (SELECT new.volume*new.price
            WHERE (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > new.price) < 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS)
            AND (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) + 0.25 * new.volume) <= (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > new.price))

          + (SELECT (CASE WHEN SUM(sub.total) IS NULL THEN 0 ELSE SUM(sub.total) END)
            FROM (
                SELECT price, sum(price * volume) as total
                FROM BIDS
                GROUP BY price
                HAVING ((SELECT (CASE WHEN SUM(VOLUME) IS NULL THEN 0 ELSE SUM(VOLUME) END) FROM BIDS b WHERE b.price > price) + (case when price < new.price then new.volume else 0 end)) < (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) + 0.25 * new.volume)
                AND 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) <= (SELECT (CASE WHEN SUM(VOLUME) IS NULL THEN 0 ELSE SUM(VOLUME) END) FROM BIDS b WHERE b.price > price)
            ) AS sub)

          - (SELECT (CASE WHEN SUM(sub.total) IS NULL THEN 0 ELSE SUM(sub.total) END)
            FROM (
                SELECT price, sum(price * volume) as total
                FROM BIDS
                GROUP BY price
                HAVING ((SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS b WHERE b.price > price) < (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS)))
                AND (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) + 0.25 * new.volume) <= ((SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS b WHERE b.price > price) + (case when price < new.price then new.volume else 0 end))
            ) AS sub);

        return new;
    END;
$on_insert_bidsf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_delete_bidsf() RETURNS TRIGGER AS $on_delete_bidsf$
    BEGIN
        UPDATE RESULTS SET total = total -
            (SELECT old.volume*old.price
            WHERE (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > old.price) < 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS))

          - (SELECT old.volume*old.price
            WHERE ((SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > old.price) < 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) - 0.25 * old.volume)
            AND 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) <= (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > old.price))

          + (SELECT old.volume*old.price
            WHERE (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > old.price) < 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS)
            AND (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) - 0.25 * old.volume) <= (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS WHERE price > old.price))

          + (SELECT (CASE WHEN SUM(sub.total) IS NULL THEN 0 ELSE SUM(sub.total) END)
            FROM (
                SELECT price, sum(price * volume) as total
                FROM BIDS
                GROUP BY price
                HAVING ((SELECT (CASE WHEN SUM(VOLUME) IS NULL THEN 0 ELSE SUM(VOLUME) END) FROM BIDS b WHERE b.price > price) - (case when price < old.price then old.volume else 0 end)) < (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) - 0.25 * old.volume)
                AND 0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) <= (SELECT (CASE WHEN SUM(VOLUME) IS NULL THEN 0 ELSE SUM(VOLUME) END) FROM BIDS b WHERE b.price > price)
            ) AS sub)

          - (SELECT (CASE WHEN SUM(sub.total) IS NULL THEN 0 ELSE SUM(sub.total) END)
            FROM (
                SELECT price, sum(price * volume) as total
                FROM BIDS
                GROUP BY price
                HAVING ((SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS b WHERE b.price > price) < (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS)))
                AND (0.25 * (SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS) - 0.25 * old.volume) <= ((SELECT (CASE WHEN SUM(volume) IS NULL THEN 0 ELSE SUM(volume) END) FROM BIDS b WHERE b.price > price) - (case when price < old.price then old.volume else 0 end))
            ) AS sub);

        return old;
    END;
$on_delete_bidsf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_bids AFTER INSERT ON BIDS
    FOR EACH ROW EXECUTE PROCEDURE on_insert_bidsf();

CREATE TRIGGER on_delete_bids AFTER delete ON BIDS
    FOR EACH ROW EXECUTE PROCEDURE on_delete_bidsf();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER on_insert_bids ON BIDS;
DROP TRIGGER on_delete_bids ON BIDS;
