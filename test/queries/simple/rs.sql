CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');
  
CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/s.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT sum(A*C) FROM R,S WHERE R.B=S.B;
