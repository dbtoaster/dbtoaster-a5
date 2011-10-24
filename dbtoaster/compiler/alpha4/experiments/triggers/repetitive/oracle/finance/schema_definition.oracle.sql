CREATE TABLE ASKS (
    t           float,
    id          integer,
    broker_id   integer,
    volume      float,
    price       float
);

CREATE TABLE BIDS (
    t           float,
    id          integer,
    broker_id   integer,
    volume      float,
    price       float
);

CREATE TABLE AGENDA (
    schema      character varying (20),
    event       integer,
    t           float,
    id          integer,
    broker_id   integer,
    volume      float,
    price       float
);

CREATE OR REPLACE PROCEDURE dispatch(log_dir IN VARCHAR2, log_file_name IN VARCHAR2)
AS
    item AGENDA%ROWTYPE;

    tally number;
    result_count number;
    total_count number;
    
    CURSOR agenda_iterator IS SELECT * FROM AGENDA;

    ts varchar2(64);
    log_file UTL_FILE.FILE_TYPE;
BEGIN
    log_file := UTL_FILE.FOPEN(log_dir, log_file_name, 'A');
    tally := 0;
    select count(*) into total_count from agenda;

    OPEN agenda_iterator;
    LOOP
        FETCH agenda_iterator into item;
        EXIT WHEN agenda_iterator%NOTFOUND;

        tally := tally + 1;
        EXECUTE IMMEDIATE 'SELECT count(*) FROM RESULTS' INTO result_count;
        
        SELECT TO_CHAR (SYSTIMESTAMP, 'MM-DD-YYYY HH24:MI:SS:FF') INTO ts FROM DUAL;
        UTL_FILE.PUT_LINE(log_file,
          ts || ',' || to_char(tally) || ','
          || to_char(total_count) || ',' || to_char(result_count));

        UTL_FILE.FFLUSH(log_file);

        case item.schema

          when 'ASKS'
          then case item.event

            when 1
            then INSERT INTO ASKS values
              (item.t, item.id, item.broker_id, item.volume, item.price);
            
            when 0
            then DELETE FROM ASKS WHERE id = item.id AND broker_id = item.broker_id;

          end case;

          when 'BIDS'
          then case item.event

            when 1
            then INSERT INTO BIDS values
              (item.t, item.id, item.broker_id, item.volume, item.price);
            
            when 0
            then DELETE FROM BIDS WHERE id = item.id AND broker_id = item.broker_id;

          end case;
        end case;
    END LOOP;

    CLOSE agenda_iterator;
    UTL_FILE.FCLOSE(log_file);
END dispatch;

/

host sqlldr @@USER@@/@@PASSWORD@@ control=events.ctl parallel=true silent=feedback
exit