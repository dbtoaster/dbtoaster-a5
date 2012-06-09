CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');
  
SELECT 1 WHERE 5.0 IN (SELECT r3.A*1.0 FROM R r3);
