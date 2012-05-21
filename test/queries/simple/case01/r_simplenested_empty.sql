
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT A, B, (A + B) FROM R WHERE A = (SELECT 3) AND A > 2;
