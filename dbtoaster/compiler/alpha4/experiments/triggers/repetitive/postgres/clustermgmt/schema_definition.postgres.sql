DROP TABLE IF EXISTS SERVER;
CREATE TABLE SERVER (
    rackid      integer,
    load        float
);

DROP TABLE IF EXISTS AGENDA;
CREATE TABLE AGENDA (
    event       integer,
    rackid      integer,
    load        float
);

CREATE OR REPLACE FUNCTION dispatch() RETURNS void AS $dispatch$
    DECLARE
        item AGENDA%ROWTYPE;

        tally integer;
        result_count integer;
        total_count integer;
    BEGIN
        tally := 0;
        total_count := (SELECT count(*) FROM AGENDA);

        FOR item in SELECT * FROM AGENDA LOOP

            tally := tally + 1;
            result_count := (select count(*) from RESULTS);
            RAISE NOTICE 'OPERATION % %/% (%): %',
              clock_timestamp(), tally, total_count, result_count, item.event;

            case 
              when item.event = 1
              then INSERT INTO SERVER values (item.rackid, item.load);
            
              when item.event = 0
              then DELETE FROM SERVER
                   WHERE rackid = item.rackid AND load = item.load;

            end case;
        END LOOP;
    END;
$dispatch$ LANGUAGE plpgsql;
