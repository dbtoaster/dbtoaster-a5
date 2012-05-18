CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', eventtype := 'insert');

SELECT A, COUNT(*) AS C FROM R GROUP BY C