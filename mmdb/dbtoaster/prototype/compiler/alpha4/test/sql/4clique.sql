CREATE TABLE E (a int, b int);

--   .-----.
--   |\   /|
--   | \ / |
--   |  X  |
--   | / \ |
--   |/   \|
--   .-----.

SELECT sum(1)
FROM   E left, E right, E top, E bottom, E nwse, E swne
WHERE  left.a =top.a     AND left.a =nwse.a
AND    left.b =bottom.a  AND left.b =swne.a
AND    right.a=top.b     AND right.a=swne.b
AND    right.b=bottom.b  AND right.b=nwse.b;

