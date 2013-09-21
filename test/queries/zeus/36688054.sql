CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT T.C AS A0_Gkg, T.D AS gCO8X, (R.B*(81.76*0*R.B)*8) AS s5aUM, T.D AS DjLJ1Lhj, R.B AS zxIGA8ae, T.D AS UokRTR, (2*(T.D*5*R.A)*83.08) AS UEw7K, 0 AS AziJbpNH, 5 AS gA8OE, T.C AS uiQXSSyS FROM R, T WHERE R.A <= (7*R.A);