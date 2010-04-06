
CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED CSV(schema := 'int,int');
CREATE TABLE S(B int, C int) 
  FROM FILE 'test/data/s.dat' LINE DELIMITED CSV(schema := 'int,int');
CREATE TABLE T(C int, D int)
  FROM FILE 'test/data/t.dat' LINE DELIMITED CSV(schema := 'int,int');

SELECT sum(A*D) FROM R,S,T WHERE R.B=S.B AND S.C=T.C;
