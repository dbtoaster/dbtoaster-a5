CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT SUM(r1.A) FROM R r1 WHERE r1.A = r1.B AND r1.A <= r1.B