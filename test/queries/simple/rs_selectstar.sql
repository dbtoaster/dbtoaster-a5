-- Expected result: 
-- 1,1 -> 1
-- 1,3 -> 3
-- 2,1 -> 2
-- 2,3 -> 6
-- 3,1 -> 1
-- 3,3 -> 3
-- 3,4 -> 1
-- 4,1 -> 1
-- 4,2 -> 2
-- 4,3 -> 5
-- 4,4 -> 1
-- 4,5 -> 1
-- 5,1 -> 2
-- 5,2 -> 2
-- 5,3 -> 6
-- 5,4 -> 1
-- 5,5 -> 1


CREATE STREAM R(A string, B string) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'string,string', eventtype := 'insert');

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT * FROM R, S;