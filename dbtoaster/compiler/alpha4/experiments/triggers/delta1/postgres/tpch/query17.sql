TRUNCATE TABLE PART;
TRUNCATE TABLE LINEITEM;

DROP TABLE IF EXISTS RESULTS;
CREATE TABLE RESULTS (
    total   float 
);

INSERT INTO RESULTS VALUES (0);

CREATE OR REPLACE FUNCTION on_insert_lineitemf() RETURNS TRIGGER AS $on_insert_lineitemf$
    BEGIN
        update results set total = total +
            (case when new.quantity < (0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem where partkey = new.partkey)) then new.extendedprice * (select count(*) from PART where partkey = new.partkey) else 0 end) +

            (case when (new.quantity < ((0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem where partkey = new.partkey)) + (0.005 * new.quantity))) and ((0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem where partkey = new.partkey)) <= new.quantity) then (new.extendedprice * (select count(*) from PART where partkey = new.partkey)) else 0 end) -

            (case when ((0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem where partkey = new.partkey) + 0.005 * new.quantity) <= new.quantity) and new.quantity < (0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem where partkey = new.partkey)) then (new.extendedprice * (select count(*) from PART where partkey = new.partkey)) else 0 end) +

        (select (case when sum(total) is NULL then 0 else sum(total) end)
        from (
            select lineitem.partkey, quantity, (case when sum(extendedprice) is NULL then 0 else sum(extendedprice) end) as total
            from lineitem, part
            where lineitem.partkey = part.partkey
            group by lineitem.partkey, quantity
            having quantity < (0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem l where l.partkey = partkey) + (case when new.partkey = lineitem.partkey then 0.005*new.quantity else 0 end))
            and (0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem l where l.partkey = partkey)) <= quantity
        ) as sub) -

        (select (case when sum(total) is NULL then 0 else sum(total) end)
        from (
            select lineitem.partkey, quantity, (case when sum(extendedprice) is NULL then 0 else sum(extendedprice) end) as total
            from lineitem, part
            where lineitem.partkey = part.partkey
            group by lineitem.partkey, quantity
            having (quantity < (0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem l where l.partkey = partkey)))
            and (0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem l where l.partkey = partkey) + (case when lineitem.partkey = new.partkey then 0.005 * new.quantity else 0 end)) <= quantity
        ) as sub);

        return new;
    END;
$on_insert_lineitemf$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION on_insert_partf() RETURNS TRIGGER AS $on_insert_partf$
    BEGIN
        update results set total = total +
            (select (case when sum(total) is NULL then 0 else sum(total) end)
            from (
                select quantity, (case when sum(extendedprice) is NULL then 0 else sum(extendedprice) end) as total
                from lineitem
                where partkey = new.partkey
                group by quantity
                having quantity < (0.005 * (select (case when sum(quantity) is NULL then 0 else sum(quantity) end) from lineitem where partkey = new.partkey))
            ) as sub);

        return new;
    END;
$on_insert_partf$ LANGUAGE plpgsql;

CREATE TRIGGER on_insert_lineitem AFTER INSERT ON LINEITEM
    FOR EACH ROW EXECUTE PROCEDURE on_insert_lineitemf();

CREATE TRIGGER on_insert_part AFTER INSERT ON PART
    FOR EACH ROW EXECUTE PROCEDURE on_insert_partf();

SELECT dispatch();

SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP TRIGGER on_insert_part ON PART;
DROP TRIGGER on_insert_lineitem ON LINEITEM;
