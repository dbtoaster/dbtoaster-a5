-- Correct answer for test data
-- Task.ttid, Assigned, Available
-- 45,10,3
-- 93,10,3
-- 120,10,4
-- 167,10,4
-- 188,10,3
-- 262,10,4
-- 268,10,4
-- 357,10,4
-- 364,10,4
-- 424,10,4
-- 449,10,3
-- 554,10,3
-- 637,10,4
-- 649,10,4
-- 690,10,4
-- 696,10,4
-- 732,10,4
-- 884,10,3
-- 885,10,4
-- 919,10,4
-- 959,10,4
-- 961,10,4
-- 993,10,4
-- 997,10,4

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

SELECT ttid, SUM(1)
FROM   Task
WHERE  (SELECT SUM(status) FROM Assignment,Server 
        WHERE ttid = atid AND asid = ssid) * 0.5 > 
       (SELECT SUM(status) FROM Assignment,Server
        WHERE ttid = atid AND asid = ssid AND status = 1)
GROUP BY ttid