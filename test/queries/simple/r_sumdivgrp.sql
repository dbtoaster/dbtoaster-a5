CREATE STREAM R(A int, B int) 
  FROM FILE 'r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT A, SUM(B)/A FROM R GROUP BY A;
