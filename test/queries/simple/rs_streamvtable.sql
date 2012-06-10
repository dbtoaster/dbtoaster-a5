CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', eventtype := 'insert');

CREATE TABLE S(B int, C int)
FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
csv (fields := ',', eventtype := 'insert');

SELECT * FROM R NATURAL JOIN S;
