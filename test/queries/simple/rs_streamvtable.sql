CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
CSV (fields := ',');

CREATE TABLE S(B int, C int)
FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
CSV (fields := ',');

SELECT * FROM R NATURAL JOIN S;
