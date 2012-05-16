CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
csv (fields := ',', schema := 'int', eventtype := 'insert');

SELECT * FROM R;