-- Expected result: 

CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT A FROM R r, (SELECT S.B, COUNT(*) FROM S GROUP BY S.B) s2 WHERE r.B < s2.B;
