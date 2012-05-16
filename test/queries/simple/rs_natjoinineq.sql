CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', eventtype := 'insert');

CREATE STREAM S(B int, C int)
FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
csv (fields := ',', eventtype := 'insert');

SELECT * FROM R NATURAL JOIN S WHERE R.A < S.C