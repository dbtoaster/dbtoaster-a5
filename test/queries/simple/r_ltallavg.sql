CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT * FROM R r2 WHERE r2.B < ALL (SELECT SUM(r1.A) / 10 FROM R r1);