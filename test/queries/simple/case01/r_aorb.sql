
CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT A + 1 FROM R WHERE A = 1 OR B = 3;
