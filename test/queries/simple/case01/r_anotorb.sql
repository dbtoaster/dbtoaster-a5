
CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT * FROM R WHERE A = 2 OR (NOT B = 3);
