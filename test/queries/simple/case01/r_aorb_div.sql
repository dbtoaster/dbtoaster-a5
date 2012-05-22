CREATE STREAM R(A int, B int)
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT A / 2, B / 3 FROM R WHERE R.A = 1 OR R.B = 2;
