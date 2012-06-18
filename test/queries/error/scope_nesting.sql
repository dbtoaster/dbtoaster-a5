
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT *
FROM R
WHERE (SELECT SUM(B) AS C FROM R) > (SELECT SUM(C) FROM R)
