CREATE TABLE Server(ssid int, status int);
CREATE TABLE Task(ttid int, priority int);
CREATE TABLE Assignment(asid int, atid int);

COPY Server     FROM '@@PATH@@/test/data/ca_servers.dat'     WITH DELIMITER ','; 
COPY Task       FROM '@@PATH@@/test/data/ca_tasks.dat'       WITH DELIMITER ',';
COPY Assignment FROM '@@PATH@@/test/data/ca_assignments.dat' WITH DELIMITER ',';

SELECT ttid, SUM(1)
FROM   Task
WHERE  (SELECT SUM(status) FROM Assignment,Server 
        WHERE ttid = atid AND asid = ssid) * 0.5 > 
       (SELECT SUM(status) FROM Assignment,Server
        WHERE ttid = atid AND asid = ssid AND status = 1)
GROUP BY ttid;

DROP TABLE Server;
DROP TABLE Task;
DROP TABLE Assignment;