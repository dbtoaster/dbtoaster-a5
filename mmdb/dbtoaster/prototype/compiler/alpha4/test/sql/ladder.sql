CREATE TABLE R (a int, b int);

-- ladder graph query

-- v0.A=t1.A -  t1 - t1.B=v1.A=t2.A - t2 - t2.B=v2.A=t3.A - t3 - t3.B=v3.A
--    |                    |                     |                   |
--    v0                   v1                    v2                  v3
--    |                    |                     |                   |
-- v0.A=b1.A -  b1 - b1.B=v1.A=b2.A - b2 - b2.B=v2.A=b3.A - b3 - b3.B=v3.A

-- the delta queries will be monsters because we use R so often
-- (~2^10 statments?)

SELECT sum(1)
FROM         R t1,       R t2,       R t3,
       R v0,       R v1,       R v2,       R  v3,
             R b1,       R b2,       R b3
WHERE t1.a = v0.a AND b1.a = v0.b
AND   t1.b = t2.a AND t1.b = v1.a
AND   b1.b = b2.a AND b1.b = v1.b
AND   t2.b = t3.a AND t2.b = v2.a
AND   b2.b = b3.a AND b2.b = v2.b
AND   t3.b = v3.a AND b3.b = v3.b;

