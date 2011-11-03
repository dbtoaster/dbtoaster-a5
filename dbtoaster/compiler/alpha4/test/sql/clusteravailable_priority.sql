/*
 priority | sum 
----------+-----
        2 |  10
        1 |   7
        0 |   7
(3 rows)
*/

CREATE TABLE Server(ssid int, status int)
  FROM FILE '../../experiments/data/ca_servers.dat'
  LINE DELIMITED csv (
    fields := ',', schema := 'int,int', eventtype := 'insert'
  );

CREATE TABLE Task(ttid int, priority int)
  FROM FILE '../../experiments/data/ca_tasks.dat'
  LINE DELIMITED csv (
    fields := ',', schema := 'int,int', eventtype := 'insert'
  );

CREATE TABLE Assignment(asid int, atid int)
  FROM FILE '../../experiments/data/ca_assignments.dat'
  LINE DELIMITED csv (
    fields := ',', schema := 'int,int', eventtype := 'insert'
  );


SELECT t.priority, SUM(1), SUM(s.status)
FROM   Task t, Assignment a, Server s
WHERE  t.ttid = a.atid AND a.asid = s.ssid
GROUP BY t.priority;

-- 
-- The following query doesn't work correctly at depth 1 (see #56)
-- 
-- SELECT priority, SUM(1)
-- FROM   Task, Assignment a1, Server s1
-- WHERE  (SELECT SUM(1) FROM Assignment a2,Server s2
--         WHERE ttid = a2.atid AND a2.asid = s2.ssid) * 0.5 > 
--        (SELECT SUM(s3.status) FROM Assignment a3,Server s3
--         WHERE ttid = a3.atid AND a3.asid = s3.ssid)
-- GROUP BY priority;