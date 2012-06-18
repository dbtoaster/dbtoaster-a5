CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
CSV (fields := ',');

CREATE STREAM S(B int, C int)
FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
CSV (fields := ',');

SELECT * FROM R NATURAL JOIN S WHERE R.A < S.C