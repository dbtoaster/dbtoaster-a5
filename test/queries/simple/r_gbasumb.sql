-- Expected result: 
-- 1 -> 3
-- 2 -> 14
-- 3 -> 12
-- 4 -> 36
-- 5 -> 40

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT A, A*SUM(B) FROM R GROUP BY A;