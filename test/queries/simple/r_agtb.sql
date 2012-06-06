
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT r1.A, r2.B
FROM R r1, R r2
WHERE r1.B > r2.A