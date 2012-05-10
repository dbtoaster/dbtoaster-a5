
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT *
FROM
  ( SELECT D, SUM(C)
    FROM 
      ( SELECT C, SUM(A) AS D
        FROM
          ( SELECT A, SUM(B) AS C
            FROM R
            GROUP BY A ) x
        GROUP BY C
      ) y
    GROUP BY D
  ) z;
