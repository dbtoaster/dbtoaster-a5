CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
CSV (fields := ',');

SELECT SUM(A) FROM R WHERE A = B AND A <= B
