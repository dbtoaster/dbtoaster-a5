-- Expected result: 1.

CREATE STREAM R(A float, B float) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT SUM(A)/(1+SUM(B)) FROM R
