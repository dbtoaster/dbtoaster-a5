CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT A / 2.0 FROM R WHERE NOT EXISTS (SELECT 2);
