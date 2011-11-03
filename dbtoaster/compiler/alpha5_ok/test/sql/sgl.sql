-- EXPECTED RESULTS FOR TEST DATA test/data/sgl.dat
--  player | sum 
-- --------+-----
--       1 |   1
--       5 |   1
--       6 |   1
--       7 |   1
--       8 |   2
--       9 |   2
--      10 |   2
--      11 |   2
--      12 |   3
--      13 |   2
--      14 |   2
--      15 |   5
--      16 |   5
--      17 |   4
--      18 |   4
--      19 |   4
--      20 |   5
--      21 |   2
--      22 |   5
--      23 |   7
--      25 |   4
--      26 |   7
--      28 |   6
--      29 |   4
--      30 |   5
--      31 |   5
--      32 |   7
--      33 |   3
--      34 |   8
--      35 |   1
--      38 |   5
--      39 |   4
--      40 |   1
--      41 |  10
--      42 |  10
--      43 |   1
--      44 |   5
--      45 |  11
--      46 |   2
--      47 |   8
--      48 |  10
--      49 |   3
--      50 |  12
--      51 |  11
--      52 |   7
--      53 |  11
--      54 |  12
--      55 |  11
--      56 |  10
--      57 |  15



CREATE TABLE E (x float, y float, player int)
  FROM FILE 'test/data/sgl.dat'
  LINE DELIMITED csv (
    fields := ',', 
    schema := 'hash,float,float,int', 
    events := '+:insert,-:delete'
  );

SELECT E2.player, sum(1)
FROM E E1, E E2
WHERE E1.x >= E2.x - 5
AND E1.x <= E2.x + 5
AND E1.y >= E2.y - 5
AND E1.y <= E2.y + 5
AND E1.player < E2.player
GROUP BY E2.player;
