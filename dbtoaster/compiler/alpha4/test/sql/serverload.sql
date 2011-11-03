CREATE TABLE Server(rackid int, load float)
  FROM FILE '../../experiments/data/sl_servers.dat'
  LINE DELIMITED csv (
    fields := ',', schema := 'event,int,float'
  );

SELECT s1.rackid, SUM(1)
FROM   Server s1
WHERE (SELECT SUM(s2.load) FROM Server s2) * 1.5
        < 
      (SELECT SUM(1) FROM Server s3) * s1.load
GROUP BY s1.rackid;