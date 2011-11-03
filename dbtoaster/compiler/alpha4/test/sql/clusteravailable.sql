/*
 task | available 
------+-----------
  120 |         4
  959 |         4
  732 |         4
  262 |         4
  268 |         4
  449 |         3
   93 |         3
  961 |         4
  554 |         3
  357 |         4
  424 |         4
  167 |         4
  696 |         4
  993 |         4
  919 |         4
  885 |         4
  997 |         4
  364 |         4
   45 |         3
  690 |         4
  884 |         3
  649 |         4
  188 |         3
  637 |         4
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

SELECT ttid, SUM(s1.status)
FROM   Task, Assignment a1, Server s1
WHERE  (SELECT SUM(1) FROM Assignment a2,Server s2
        WHERE ttid = a2.atid AND a2.asid = s2.ssid) * 0.5 > 
       (SELECT SUM(1) FROM Assignment a3,Server s3
        WHERE ttid = a3.atid AND a3.asid = s3.ssid AND s3.status = 1)
  AND  ttid = a1.atid 
  AND  a1.asid = s1.ssid
GROUP BY ttid;