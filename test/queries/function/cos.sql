CREATE STREAM RadialMeasurements ( angle double, distance double )
  FROM FILE '../dbtoaster-experiments-data/simple/standard/r.dat' LINE DELIMITED CSV;


CREATE FUNCTION cos ( x double ) RETURNS int AS EXTERNAL 'cos';
CREATE FUNCTION sin ( x double ) RETURNS int AS EXTERNAL 'sin';

SELECT r.distance * cos(r.angle) AS x, 
       r.distance * sin(r.angle) AS y
FROM RadialMeasurements r;