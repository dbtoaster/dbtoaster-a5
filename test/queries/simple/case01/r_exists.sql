CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT R1.A / 2.0 FROM R AS R1 WHERE EXISTS (SELECT 3);
