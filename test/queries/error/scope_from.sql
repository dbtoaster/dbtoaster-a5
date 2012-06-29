
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT *
FROM R, (SELECT SUM(B) AS C FROM R) S, (SELECT SUM(C) FROM R) T
