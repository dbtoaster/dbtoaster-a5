CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT B, sum(A) FROM R group by B;
