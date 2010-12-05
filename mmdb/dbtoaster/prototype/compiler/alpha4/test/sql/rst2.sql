-- With the datafiles in test/data, this should produce
-- QUERY_1_1 = 18753367048934
-- or as the Ocaml outputter puts it:
-- QUERY_1_1: [[  ]->[[  ]->1.87533670489e+13;]<pat=>;]<pat=>

CREATE TABLE R(A int, B int) 
  FROM POSTGRES dbtoaster.R(A int, B int);
CREATE TABLE S(B int, C int) 
  FROM POSTGRES dbtoaster.S(B int, C int);
CREATE TABLE T(C int, D int)
  FROM POSTGRES dbtoaster.T(C int, D int);

SELECT sum(A*D) FROM R,S,T WHERE R.B<S.B AND S.C<T.C;
