DROP TABLE IF EXISTS input;

CREATE TABLE input (
 	type integer,
 	time integer, carid integer,
 	speed integer, xway integer, lane integer, dir integer,
 	seg integer, pos integer,
 	qid integer,
 	m_init integer, m_end integer,
 	dow integer, tod integer, day integer );
 	
-- xwayLoop.pl

-- Note the following needs to loop over each xway
-- see xwayLoop.pl in validator code
-- each xway is extracted into the 'input' table, and processed

/*
INSERT INTO input SELECT * FROM xwayinput WHERE xway = xxx
*/


CREATE INDEX idx1 ON input (type, lane);
CREATE INDEX idx2 ON input (type);
CREATE INDEX idx3 ON input (type, speed);

--CREATE INDEX idx4 ON input (xway, dir, seg);
CREATE INDEX idx4 ON input (dir, seg);

-- CREATE INDEX idx5 ON input (xway, dir, seg, time);
-- CREATE INDEX idx5 ON input (dir, seg, time);

-- CREATE INDEX idx6 ON input (xway, dir, seg, carid);
CREATE INDEX idx6 ON input (dir, seg, carid);

-- CREATE INDEX idx7 ON input (xway, dir, seg, carid, time);
-- CREATE INDEX idx7 ON input (dir, seg, carid, time);

-- CREATE INDEX idx8 ON input (xway, dir, seg, type);
CREATE INDEX idx8 ON input (dir, seg, type); 

-- CREATE INDEX idx9 ON input (xway, dir, seg, type, time);
-- CREATE INDEX idx9 ON input (dir, seg, type, time);

DROP TABLE IF EXISTS accident;

CREATE TABLE accident(
	carid1 integer,
	carid2 integer,
	firstMinute integer,
	lastMinute integer,
	dir integer,
	seg integer,
	pos integer,
	PRIMARY KEY ( dir, pos, firstMinute) );

CREATE TABLE preAccident1(
	time integer,
	carid integer,
	lane integer,
	dir integer,
	seg integer,
	pos integer );

CREATE TABLE preAccident2(
	carid1 integer,
	carid2 integer,
	time integer,
	dir integer,
	seg integer,
	pos integer );

INSERT INTO preAccident1
SELECT  time, carid,
        lane, dir, seg, pos
FROM input
WHERE speed = 0 AND type = 0;

CREATE INDEX preAcc1Idx1 ON preAccident1 (carid, pos, lane, dir);
CREATE INDEX preAcc1Idx2 ON preAccident1 (carid, pos, lane, dir, time);
CREATE INDEX preAcc1Idx3 ON preAccident1 (pos, lane, dir);
CREATE INDEX preAcc1Idx4 ON preAccident1 (pos, lane, dir, time);

INSERT INTO preAccident2
SELECT  in1.carid, in2.carid, in2.time,
		in1.dir, in1.seg, in1.pos
FROM    preAccident1 AS in1,
		preAccident1 AS in2,
		preAccident1 AS in11,
		preAccident1 AS in22
WHERE   in2.carid <> in1.carid AND
		in2.pos = in1.pos AND                       
		in2.lane = in1.lane AND
		in2.dir = in1.dir AND
		in2.time >= in1.time AND 
		in2.time <= in1.time + 30 AND
		in11.carid = in1.carid AND
		in11.pos = in1.pos AND                       
		in11.lane = in1.lane AND
		in11.dir = in1.dir AND
		in11.time = in1.time + 120 AND
		in22.carid = in2.carid AND
		in22.pos = in1.pos AND                       
		in22.lane = in1.lane AND
		in22.dir = in1.dir AND
		in22.time = in2.time + 120;
		
INSERT INTO accident
SELECT   min(carid1),
		 max(carid2),
		 trunc((min(time) + 90)/60) + 1, 
		 trunc((max(time) + 120)/60) + 1,
		 dir, seg, pos
FROM     preAccident2
GROUP BY dir, seg, pos;

-- clean up accident extraction
DROP TABLE preAccident1;
DROP TABLE preAccident2;

