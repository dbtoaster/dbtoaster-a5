CREATE TABLE E (a int, b int);

--   .-----.
--   |\   /|
--   | \ / |
--   |  X  |
--   | / \ |
--   |/   \|
--   .-----.

SELECT sum(1)
FROM   E eleft, E eright, E etop, E ebottom, E enwse, E eswne
WHERE  eleft.a =etop.a     AND eleft.a =enwse.a
AND    eleft.b =ebottom.a  AND eleft.b =eswne.a
AND    eright.a=etop.b     AND eright.a=eswne.b
AND    eright.b=ebottom.b  AND eright.b=enwse.b;

