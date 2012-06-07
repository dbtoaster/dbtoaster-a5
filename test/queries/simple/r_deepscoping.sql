CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT r3.A
FROM (SELECT r1.A FROM R r1,
(SELECT r2.A FROM R r2 WHERE r1.A=r2.A) r2b) r3,
R r4
WHERE r3.A = r4.A
