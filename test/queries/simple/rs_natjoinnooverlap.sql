CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int', eventtype := 'insert');

CREATE STREAM S(C int, D int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int', eventtype := 'insert');

SELECT R.* FROM R NATURAL JOIN S;