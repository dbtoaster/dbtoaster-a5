CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT r1.A FROM R r1, R r2 WHERE (r1.A = r2.A or r1.A = r2.A) and r1.B = r2.B;
