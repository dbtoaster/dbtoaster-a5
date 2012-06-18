-- Expected result: 


CREATE STREAM R(A string, B string) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT * FROM R;
