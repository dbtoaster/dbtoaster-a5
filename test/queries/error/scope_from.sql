
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT *
FROM R, (SELECT SUM(B) AS C FROM R) S, (SELECT SUM(C) FROM R) T
