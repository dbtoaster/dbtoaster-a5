CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
CSV (fields := ',');

SELECT * FROM R WHERE R.A <= R.B AND R.A >= R.B;
