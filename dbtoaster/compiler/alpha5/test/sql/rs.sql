CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED csv (fields := ',', schema := 'int,int', eventtype := 'insert');
CREATE TABLE S(B int, C int) 
  FROM FILE 'test/data/s.dat' LINE DELIMITED csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT sum(A*C) FROM R,S WHERE R.B=S.B;
