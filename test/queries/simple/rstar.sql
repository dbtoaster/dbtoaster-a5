
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

--CREATE STREAM S(B int, C int) 
--  FROM FILE '../../experiments/data/s.dat' LINE DELIMITED
--  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT sum(1) 
FROM  R ra, R rb, R rc 
WHERE ra.A = rb.A AND
      rb.A = rc.A;

--SELECT sum(1) 
--FROM --R ra, 
--     R rb, 
--     R rc--, 
--     --S sa, 
--     --S sb, 
--     --S sc
--WHERE --ra.A = rb.A AND 
--      rb.A = rc.A-- AND
--      --ra.B = sa.B AND 
--      --rb.B = sb.B AND 
--      --rc.B = sc.B
----GROUP BY --sb.B, 
--         --sc.C
--;
