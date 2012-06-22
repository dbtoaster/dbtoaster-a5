-- Expected result: 1.

CREATE STREAM R(A float, B float) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT SUM(A)/(1+SUM(B)) FROM R
