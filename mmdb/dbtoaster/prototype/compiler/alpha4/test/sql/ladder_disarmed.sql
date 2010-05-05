CREATE TABLE R1 (a int, b int);
CREATE TABLE R2 (a int, b int);
CREATE TABLE R3 (a int, b int);
CREATE TABLE R4 (a int, b int);
CREATE TABLE R5 (a int, b int);
CREATE TABLE R6 (a int, b int);
CREATE TABLE R7 (a int, b int);
CREATE TABLE R8 (a int, b int);
CREATE TABLE R9 (a int, b int);
CREATE TABLE R0 (a int, b int);

-- ladder graph query

-- v0.A=t1.A -  t1 - t1.B=v1.A=t2.A - t2 - t2.B=v2.A=t3.A - t3 - t3.B=v3.A
--    |                    |                     |                   |
--    v0                   v1                    v2                  v3
--    |                    |                     |                   |
-- v0.A=b1.A -  b1 - b1.B=v1.A=b2.A - b2 - b2.B=v2.A=b3.A - b3 - b3.B=v3.A

SELECT sum(1)
FROM          R1 t1,       R2 t2,        R3 t3,
       R4 v0,       R5 v1,        R6 v2,        R7  v3,
              R8 b1,       R9 b2,        R0 b3
WHERE t1.a = v0.a AND b1.a = v0.b
AND   t1.b = t2.a AND t1.b = v1.a
AND   b1.b = b2.a AND b1.b = v1.b
AND   t2.b = t3.a AND t2.b = v2.a
AND   b2.b = b3.a AND b2.b = v2.b
AND   t3.b = v3.a AND b3.b = v3.b;

