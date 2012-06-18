CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT A / 2, B / 3 FROM R WHERE R.A = 1 OR R.B = 2;
