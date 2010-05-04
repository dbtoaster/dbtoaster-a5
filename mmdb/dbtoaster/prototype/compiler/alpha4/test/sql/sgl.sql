CREATE TABLE E (x int, y int, player int);

SELECT E2.player, sum(1)
FROM E E1, E E2
WHERE E1.x >= E2.x - 5
AND E1.x <= E2.x + 5
AND E1.y >= E2.y - 5
AND E1.y <= E2.y + 5
AND E1.player < E2.player
GROUP BY E2.player;
