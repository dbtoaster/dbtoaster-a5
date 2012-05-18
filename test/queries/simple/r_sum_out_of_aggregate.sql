CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', eventtype := 'insert');

SELECT 1+SUM(A) FROM R