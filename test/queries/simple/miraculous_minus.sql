CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT r1.A FROM R r1 WHERE r1.A =
(SELECT SUM(r2.B) FROM R r2 WHERE r2.A = r1.B);
