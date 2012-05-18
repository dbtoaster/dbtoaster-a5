CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', eventtype := 'insert');

SELECT *
FROM (SELECT R.A, COUNT(*) as C FROM R GROUP BY R.A) s
WHERE s.A = 3