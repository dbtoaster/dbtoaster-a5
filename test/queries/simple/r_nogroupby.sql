-- Expected results
-- 1 -> 1
-- 2 -> 3
-- 3 -> 1
-- 4 -> 3
-- 5 -> 2

CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT A FROM R;