CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');
  
CREATE TABLE S(B int, C int) 
  FROM FILE '../../experiments/data/s.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT C,sum(A) FROM R,S WHERE R.B<S.B group by C;
