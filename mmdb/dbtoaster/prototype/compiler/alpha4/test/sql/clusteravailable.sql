CREATE TABLE Server(ssid int, status int);
CREATE TABLE Assignment(asid int, atid int);
CREATE TABLE Task(ttid int, priority int);

SELECT ttid, SUM(1)
FROM   Task
WHERE  (SELECT SUM(status) FROM Assignment,Server 
        WHERE ttid = atid AND asid = ssid) * 0.5 > 
       (SELECT SUM(status) FROM Assignment,Server
        WHERE ttid = atid AND asid = ssid AND status = 1)
GROUP BY ttid