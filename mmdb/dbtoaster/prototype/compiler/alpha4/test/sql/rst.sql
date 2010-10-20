-- With the datafiles in test/data, this should produce
-- QUERY_1_1 = 18753367048934
-- or as the Ocaml outputter puts it:
-- QUERY_1_1: [[  ]->[[  ]->1.87533670489e+13;]<pat=>;]<pat=>

CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED csv (fields := ',');
CREATE TABLE S(B int, C int) 
  FROM FILE 'test/data/s.dat' LINE DELIMITED csv (fields := ',');
CREATE TABLE T(C int, D int)
  FROM FILE 'test/data/t.dat' LINE DELIMITED csv (fields := ',');

SELECT sum(A*D) FROM R,S,T WHERE R.B=S.B AND S.C=T.C;
