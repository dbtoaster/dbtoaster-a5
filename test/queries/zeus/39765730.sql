CREATE STREAM R(A int, B int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT uwFbkS.*, T.*, count(*) FROM S, T uwFbkS, T WHERE ((uwFbkS.D = T.C OR (T.C <> 5 AND (T.D*(S.C-56.44)) < ((0-S.B)*3) AND (NOT 0 <= uwFbkS.D)) OR 2 = (S.B*uwFbkS.D)) AND T.D <> (T.C+T.C+7)) GROUP BY uwFbkS.C, uwFbkS.D, T.C, T.D;
