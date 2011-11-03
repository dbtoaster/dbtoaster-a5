CREATE TABLE SERVER (
    rackid           integer,
    load             double
);

CREATE TABLE AGENDA (
    event            integer,
    rackid           integer,
    load             double
);

CREATE OR REPLACE PROCEDURE dispatch(log_dir IN VARCHAR2, log_file_name IN VARCHAR2)
AS
    item AGENDA%ROWTYPE;

    tally integer;
    result_count integer;
    total_count integer;
    
    CURSOR agenda_iterator IS SELECT * FROM AGENDA;

    ts varchar2(64);
    log_file UTL_FILE.FILE_TYPE;

BEGIN
	  log_file := UTL_FILE.FOPEN(log_dir, log_file_name, 'A');
    tally := 0;
    SELECT count(*) INTO total_count FROM AGENDA;
    
    OPEN agenda_iterator;
    LOOP
        FETCH agenda_iterator INTO item;
        EXIT WHEN agenda_iterator%NOTFOUND;

        tally := tally + 1;
        EXECUTE IMMEDIATE 'SELECT count(*) FROM RESULTS' INTO result_count;

        SELECT TO_CHAR (SYSTIMESTAMP, 'MM-DD-YYYY HH24:MI:SS:FF') INTO ts FROM DUAL;
        UTL_FILE.PUT_LINE(log_file,
          ts || ',' || to_char(tally) || ','
          || to_char(total_count) || ',' || to_char(result_count));

        UTL_FILE.FFLUSH(log_file);

        case item.event

          when 1
          then INSERT INTO SERVER values (item.rackid, item.load);
            
          when 0
          then DELETE FROM CUSTOMER where rackid = item.rackid AND load = item.load;

        end case;
    END LOOP;

    CLOSE agenda_iterator;
    UTL_FILE.FCLOSE(log_file);
END dispatch;

/

host sqlldr @@USER@@/@@PASSWORD@@ control=events.ctl parallel=true silent=feedback
exit