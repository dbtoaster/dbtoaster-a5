/*
 priority | sum 
----------+-----
        2 |  10
        1 |   7
        0 |   7
(3 rows)
*/

CREATE TABLE Server(ssid int, status int)
  FROM FILE 'test/data/ca_servers.dat'
  LINE DELIMITED csv (
    fields := ',', schema := 'int,int', eventtype := 'insert'
  );

CREATE TABLE Task(ttid int, priority int)
  FROM FILE 'test/data/ca_tasks.dat'
  LINE DELIMITED csv (
    fields := ',', schema := 'int,int', eventtype := 'insert'
  );

CREATE TABLE Assignment(asid int, atid int)
  FROM FILE 'test/data/ca_assignments.dat'
  LINE DELIMITED csv (
    fields := ',', schema := 'int,int', eventtype := 'insert'
  );

SELECT priority, SUM(1)
FROM   Task
WHERE  (SELECT SUM(1) FROM Assignment a2,Server s2
        WHERE ttid = a2.atid AND a2.asid = s2.ssid) * 0.5 > 
       (SELECT SUM(1) FROM Assignment a3,Server s3
        WHERE ttid = a3.atid AND a3.asid = s3.ssid AND s3.status = 1)
GROUP BY priority;