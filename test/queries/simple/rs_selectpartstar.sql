-- Expected result: 

CREATE STREAM R(A string, B string) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT R.* FROM R, S;
