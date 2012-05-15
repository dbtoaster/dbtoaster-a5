CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT * FROM R r2 WHERE r2.B > SOME (SELECT r1.A FROM R r1);