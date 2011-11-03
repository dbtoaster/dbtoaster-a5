BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RESULTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

/

CREATE TABLE RESULTS (
    rackid    integer,
    total     integer
);

CREATE OR REPLACE PROCEDURE recompute_query AS
    item RESULTS%ROWTYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESULTS';
    INSERT INTO RESULTS
      SELECT s1.rackid, SUM(1)
      FROM   Server s1
      WHERE  (SELECT SUM(s2.load) FROM Server s2) * 1.5
             < (SELECT SUM(1) FROM Server s3) * s1.load
      GROUP BY s1.rackid;
    COMMIT;
END;

/

CREATE OR REPLACE TRIGGER refresh_server
  AFTER INSERT OR DELETE ON SERVER
  FOR EACH ROW
BEGIN EXECUTE IMMEDIATE 'CALL recompute_query()'; END;

/

CREATE DIRECTORY svllog AS '/tmp';
CALL dispatch('SVLLOG', 'serverload.log');
SELECT * FROM RESULTS;

DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY svllog;
DROP TRIGGER refresh_server;

exit