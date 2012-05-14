
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT *
FROM R r1, (SELECT SUM(B) AS C FROM R) S
WHERE A > (SELECT SUM(C) FROM R r2);