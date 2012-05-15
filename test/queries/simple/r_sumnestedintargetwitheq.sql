
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT SUM((SELECT SUM(1) FROM R r2 WHERE r1.A = r2.A)) FROM R r1;