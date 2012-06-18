CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
CSV (fields := ',');

SELECT * FROM R r2 
WHERE r2.A IN (
  SELECT AVG(r1.A), B FROM R r1 WHERE r1.B = r2.B GROUP BY B
);
