
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT *
FROM R, (SELECT SUM(B) AS C FROM R) S, (SELECT SUM(C) FROM R) T