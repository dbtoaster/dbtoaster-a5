
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED csv;

SELECT SUM(1) FROM (SELECT SUM(1) FROM R) r;
