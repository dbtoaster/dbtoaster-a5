CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');
  
-- SELECT 1 WHERE (SELECT COUNT(*)/2 FROM R) IN (SELECT r3.A FROM R r3);
SELECT r1.A FROM R r1 WHERE (SELECT COUNT(*)/2 FROM R) IN (SELECT r3.A FROM R r3);

