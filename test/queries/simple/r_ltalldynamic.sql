CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
CSV (fields := ',');

SELECT * FROM R r2 WHERE r2.B < ALL (SELECT r1.A FROM R r1);
