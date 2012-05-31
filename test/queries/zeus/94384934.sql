CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/tiny_t.dat' LINE DELIMITED CSV();

SELECT T.D AS TIeEkk, 39.61 AS rCzSTVIE FROM R, T, T EL8tAX WHERE (EL8tAX.D*0) = R.B;
