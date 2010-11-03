CREATE TABLE R(A int, B int) 
  FROM POSTGRES dbtoaster.R(A int, B int);
CREATE TABLE S(C int, D int) 
  FROM POSTGRES dbtoaster.S(C int, D int);
--CREATE TABLE T(E int, F int) 
--  FROM POSTGRES dbtoaster.T(E int, F int);
--CREATE TABLE U(G int, H int) 
--  FROM POSTGRES dbtoaster.U(G int, H int);

--SELECT sum(A*H) FROM R,S,T,U WHERE R.B<S.C AND R.B<T.E AND T.F<U.G AND R.B<U.H;
SELECT sum(A*C) FROM R,S WHERE R.B<S.C;
