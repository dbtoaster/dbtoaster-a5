CREATE STREAM R(A int, B int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT 26.81 AS aSvLe7WQ, S.B AS xrWDqdt, 6 AS JAPlGDFD8 FROM S DLhX8NFLu, S, S nUvGvX WHERE 66.82 > (nUvGvX.B+nUvGvX.C);
