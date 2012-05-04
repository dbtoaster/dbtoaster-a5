-- Expected result: 1.

CREATE STREAM R(A float, B float) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT SUM(A)/(1+SUM(B)) FROM R