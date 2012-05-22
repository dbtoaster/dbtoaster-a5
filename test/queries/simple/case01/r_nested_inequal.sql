CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT R1.A / 2.0 FROM R AS R1 WHERE R1.A < (SELECT 3);

