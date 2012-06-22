CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV (fields := ',');

CREATE STREAM S(C int, D int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV (fields := ',');

SELECT R.* FROM R NATURAL JOIN S;
