
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT A, B, (A + B) FROM R WHERE A = (SELECT 3) AND A > 2;
