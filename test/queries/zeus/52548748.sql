CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT vRWwZnEB.*, R.A, T.C, T.D AS FYfBDfwr, T.D AS c3V6N, 9 AS GHQceSx, 0 AS mEQKjjoQM, (vRWwZnEB.A*R.B*R.A) AS Y2oLepE, (vRWwZnEB.A*94.75*vRWwZnEB.A) AS ntmBK, R.A AS LwHlLSt, vRWwZnEB.B AS icavaQEH, vRWwZnEB.A AS kKG8Br9sM FROM R vRWwZnEB, R, T WHERE ((((6 >= vRWwZnEB.A OR T.D >= vRWwZnEB.A) AND vRWwZnEB.A > T.D AND (R.B = T.D AND (R.A+0+R.B) <= 3.15 AND vRWwZnEB.A = R.A)) OR (9+T.D) > T.D) AND 9 = vRWwZnEB.B);
