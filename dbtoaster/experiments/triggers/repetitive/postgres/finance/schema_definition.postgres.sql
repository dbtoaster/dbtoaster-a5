DROP TABLE IF EXISTS ASKS;
CREATE TABLE ASKS (
    t           float,
    id          integer,
    broker_id   integer,
    volume      float,
    price       float
);

DROP TABLE IF EXISTS BIDS;
CREATE TABLE BIDS (
    t           float,
    id          integer,
    broker_id   integer,
    volume      float,
    price       float
);

DROP TABLE IF EXISTS AGENDA;
CREATE TABLE AGENDA (
    schema      character varying (20),
    event       integer,
    t           float,
    id          integer,
    broker_id   integer,
    volume      float,
    price       float
);

CREATE OR REPLACE FUNCTION dispatch() RETURNS void AS $dispatch$
    DECLARE
        item AGENDA%ROWTYPE;

        count integer;
        result_count integer;
        total_count integer;
    BEGIN
        count := 0;
        total_count := (SELECT count(*) FROM AGENDA);

        FOR item in SELECT * FROM AGENDA LOOP

            count := count + 1;
            result_count := (select count(*) from RESULTS);
            RAISE NOTICE 'OPERATION % %/% (%): % %', clock_timestamp(), count, total_count, result_count, item.schema, item.event;

            case 

              when item.schema = 'ASKS'
              then case

                when item.event = 1
                then INSERT INTO ASKS values
                  (item.t, item.id, item.broker_id, item.volume, item.price);
                
                when item.event = 0
                then DELETE FROM ASKS WHERE id = item.id AND broker_id = item.broker_id;

              end case;

              when item.schema = 'BIDS'
              then case

                when item.event = 1
                then INSERT INTO BIDS values
                  (item.t, item.id, item.broker_id, item.volume, item.price);
                
                when item.event = 0
                then DELETE FROM BIDS WHERE id = item.id AND broker_id = item.broker_id;

              end case;
            end case;
        END LOOP;
    END;
$dispatch$ LANGUAGE plpgsql;
