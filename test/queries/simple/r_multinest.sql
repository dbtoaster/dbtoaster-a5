
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT *
FROM
  ( SELECT y.D, SUM(y.C)
    FROM 
      ( SELECT x.C, SUM(x.A) AS D
        FROM
          ( SELECT r1.A, SUM(r1.B) AS C
            FROM R r1
            GROUP BY r1.A ) x
        GROUP BY x.C
      ) y
    GROUP BY D
  ) z;
