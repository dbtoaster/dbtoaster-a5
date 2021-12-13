CREATE STREAM R(A int, B int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT (zz0yfNEJe.C*(7*zz0yfNEJe.B*zz0yfNEJe.B)*(0+zz0yfNEJe.B+26.04)) AS ruqL5pm, (57.75*zz0yfNEJe.C*45.94) AS Mx6vEGFTf FROM S zz0yfNEJe WHERE (zz0yfNEJe.B*zz0yfNEJe.C) <> (zz0yfNEJe.C*zz0yfNEJe.B*25.23);
