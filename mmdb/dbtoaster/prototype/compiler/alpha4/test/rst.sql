
CREATE TABLE R(A double, B double) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED CSV;
CREATE TABLE S(B double, C double) 
  FROM FILE 'test/data/s.dat' LINE DELIMITED CSV;
CREATE TABLE T(C double, D double)
  FROM FILE 'test/data/t.dat' LINE DELIMITED CSV;

SELECT sum(A*D) FROM R,S,T WHERE R.B=S.B AND S.C=T.C;
