-- dropalltables.pl
-- drop basic tables
DROP TABLE IF EXISTS completeHistory;
DROP TABLE IF EXISTS input;
DROP TABLE IF EXISTS outputAccountBalance;
DROP TABLE IF EXISTS outputDailyExpenditure;

-- Tables for outputs from SPEs.
DROP TABLE IF EXISTS tollAlerts;
DROP TABLE IF EXISTS accidentAlerts;

-- drop account balance query tables
DROP TABLE IF EXISTS accountBalanceAncient;
DROP TABLE IF EXISTS accountBalanceAnswer;
DROP TABLE IF EXISTS accountBalanceLast;
DROP TABLE IF EXISTS accountBalanceMiddle;
DROP TABLE IF EXISTS accountBalanceNow;
DROP TABLE IF EXISTS accountBalanceQueryAtEntrance;
DROP TABLE IF EXISTS accountBalanceRequests;
DROP TABLE IF EXISTS accountBalanceTime0;
DROP TABLE IF EXISTS accountBalanceTime1;
DROP TABLE IF EXISTS accountBalanceTime10;
DROP TABLE IF EXISTS accountBalanceTime2;
DROP TABLE IF EXISTS accountBalanceTime60;
DROP TABLE IF EXISTS accountBalanceTimeEq;
DROP TABLE IF EXISTS accountBalanceWrongAnswers;

-- drop daily expenditure tables
DROP TABLE IF EXISTS dailyExpenditureAnswer;
DROP TABLE IF EXISTS dailyExpenditureRequests;

-- import.pl
-- create basic tables
CREATE TABLE completeHistory (
	carid integer, day integer, xway integer, toll integer );
	
CREATE TABLE input (
 	type integer,
 	time integer, carid integer,
 	speed integer, xway integer, lane integer, dir integer,
 	seg integer, pos integer,
 	qid integer,
 	m_init integer, m_end integer,
 	dow integer, tod integer, day integer );
 	
CREATE TABLE outputAccountBalance (
	type integer,
	time integer, emit integer,
	qid integer, resulttime integer, bal integer );
	
CREATE TABLE outputDailyExpenditure (
	type integer, time integer,
	emit text, qid integer, bal integer );
	
CREATE TABLE tollAlerts (
	type integer,
	carid integer, time integer,
	emit text, lav integer, toll integer );

CREATE TABLE accidentAlerts (
	type integer, time integer,
	emit text, carid integer, seg integer );

-- indexes.pl
-- create indexes on basic tables
CREATE INDEX tollAlertstime ON tollAlerts (time);
CREATE INDEX tollAlertscarid ON tollAlerts (carid);
CREATE INDEX tollAlertstoll ON tollAlerts (toll);
CREATE INDEX tollIdx1 ON tollAlerts(time, carid);

CREATE INDEX accIdx1 ON accidentAlerts(time, carid, seg);

-- xwayLoop.pl

-- for more than one xway, rename complete inputs and
-- create temporary alerts table

/*
DROP TABLE IF EXISTS tollAccAlertsTmp;

--CREATE TABLE tollAccAlertsTmp(   
--	time   integer,
--	carid  integer,
--	xway   integer,
--	dir    integer,
--	seg    integer,
--	lav    integer,
--	toll   integer,
--	accidentSeg integer);

CREATE TABLE tollAccAlertsTmp(   
	time   integer,
	carid  integer,
	dir    integer,
	seg    integer,
	lav    integer,
	toll   integer,
	accidentSeg integer);
*/

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

------------------------
--
-- generateAlerts.pl

-- runDdl.pl
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

DROP TABLE IF EXISTS statistics;
CREATE TABLE statistics(
	dir  integer,
	seg  integer,
	minute integer,
	numvehicles  integer,
	lav   integer,
	toll  integer,
	accident integer,
	accidentSeg integer);

DROP TABLE IF EXISTS tollAccAlerts;
CREATE TABLE TollAccAlerts(   
	time   integer,
	carid  integer,
	dir    integer,
	seg    integer,
	lav    integer,
	toll   integer,
	accidentSeg integer);

-- create tables for validation. we skip these for now...
/* 
DROP TABLE accAlertNotInValidator;
DROP TABLE accAlertNotInOriginal;
DROP TABLE tollAlertNotInValidator;
DROP TABLE tollAlertNotInOriginal;

CREATE TABLE accAlertNotInValidator(
	time  integer,
	carid integer,
	seg   integer);

CREATE TABLE accAlertNotInOriginal(
	time  integer,
	carid integer,
	seg   integer);

CREATE TABLE tollAlertNotInValidator(
	time  integer,
	carid integer);

CREATE TABLE tollAlertNotInOriginal(
	time  integer,
	carid integer);
*/

-----------------------
--
-- extractAccident.pl

--TRUNCATE TABLE accident;
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

# CREATE INDEX preAcc1Idx1 ON preAccident1 (carid, pos, xway, lane, dir);
# CREATE INDEX preAcc1Idx2 ON preAccident1 (carid, pos, xway, lane, dir, time);
# CREATE INDEX preAcc1Idx3 ON preAccident1 (pos, xway, lane, dir);
# CREATE INDEX preAcc1Idx4 ON preAccident1 (pos, xway, lane, dir, time);

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

---------------------------
--
-- insertStatistics.pl

-- TRUNCATE TABLE statistics;   

INSERT INTO statistics(dir, seg, minute, numvehicles)
SELECT   dir, seg,
		 trunc(time/60) + 1, 0
FROM     input
WHERE    type = 0
GROUP BY dir, seg, trunc(time/60);

-- CREATE UNIQUE INDEX statisticsIdx1 statistics (xway, dir, seg, minute);
CREATE UNIQUE INDEX statisticsIdx1 ON statistics (dir, seg, minute);

-----------------------------
--
-- extractNumVehicles.pl

/*
CREATE TABLE preVehicle(
	xway integer,
	dir  integer,
	seg  integer,
	minute integer,
	numVehicles  integer);               
*/

CREATE TABLE preVehicle(
	dir  integer,
	seg  integer,
	minute integer,
	numVehicles  integer);

-- Extract number of vehicles in an (express way, direction, segment, minute)
INSERT INTO  preVehicle(dir, seg, minute, numvehicles)
SELECT   dir, seg,
		 trunc(time/60) + 2,
		 count(distinct carid)
FROM     input
WHERE    type = 0
GROUP BY dir, seg, trunc(time/60);

CREATE UNIQUE INDEX preVehicleIdx1 ON preVehicle (dir, seg, minute);

/*
UPDATE statistics
SET numVehicles =
	(SELECT numVehicles
	 FROM   preVehicle
	 WHERE  statistics.xway = preVehicle.xway AND
			statistics.dir = preVehicle.dir AND
			statistics.seg = preVehicle.seg AND
			statistics.minute = preVehicle.minute);
*/

UPDATE statistics
SET numVehicles =
	(SELECT numVehicles
	 FROM   preVehicle
	 WHERE  statistics.dir = preVehicle.dir AND
			statistics.seg = preVehicle.seg AND
			statistics.minute = preVehicle.minute);

UPDATE statistics
SET    numVehicles = 0
WHERE  numVehicles IS NULL;

-- clean up num vehicles extraction 
DROP TABLE  preVehicle;   


---------------------------------
--
-- extractLavs.pl

/*
CREATE TABLE preLav(
	xway integer,
	dir  integer,
	seg  integer,
	minute integer,
	lav  float);
*/

CREATE TABLE preLav( 
	dir  integer,
	seg  integer,
	minute integer,
	lav  float);

-- Calculate lav for (xway, dir, seg)  at every minute

/*
INSERT INTO prelav 
SELECT xway, dir, seg,  minute, avg(speed)
FROM
	(SELECT xway AS xway,
			dir AS dir,
			seg AS seg,
			carid as carid,
			trunc(time/60) + 1 AS minute,
			avg(speed) AS speed
	 FROM      input
	 WHERE     type = 0
	 GROUP BY  xway, dir, seg, carid, minute)
	 AS filter
GROUP BY xway, dir, seg,  minute;
*/

INSERT INTO prelav 
SELECT dir, seg,  minute, avg(speed)
FROM
	(SELECT dir AS dir,
			seg AS seg,
			carid as carid,
			trunc(time/60) + 1 AS minute,
			avg(speed) AS speed
	FROM      input
	WHERE     type = 0
	GROUP BY  dir, seg, carid, minute)
AS filter
GROUP BY dir, seg,  minute;

-- CREATE UNIQUE INDEX preLavIdx1 ON preLav (xway, dir, seg, minute);
CREATE UNIQUE INDEX preLavIdx1 ON preLav (dir, seg, minute);

/*
UPDATE  statistics
SET lav =
	(SELECT trunc(avg(prelav.lav))
	 FROM   prelav
	 WHERE  statistics.xway = prelav.xway AND
			statistics.dir = prelav.dir AND
			statistics.seg = prelav.seg AND
			prelav.minute <= statistics.minute - 1 AND
			prelav.minute  >= statistics.minute - 5);
*/

UPDATE  statistics
SET lav =
	(SELECT trunc(avg(prelav.lav))
	 FROM   prelav
	 WHERE  statistics.dir = prelav.dir AND
			statistics.seg = prelav.seg AND
			prelav.minute <= statistics.minute - 1 AND
			prelav.minute >= statistics.minute - 5);

UPDATE statistics
SET    lav = -1
WHERE  lav IS NULL;


-- clean up lav extraction
DROP TABLE preLav;

----------------------------
--
-- calculateTolls.pl

UPDATE  statistics
SET     toll = 0
WHERE   lav >= 40 OR
		numVehicles <= 50;

/*
UPDATE  statistics
SET     toll = 0,
		accident = 1
WHERE EXISTS(
	SELECT  acc.seg
	FROM    accident AS acc 
	WHERE   acc.xway = statistics.xway AND
			acc.dir = statistics.dir AND
			acc.firstMinute + 1 <= statistics.minute AND
			acc.lastMinute + 1 >= statistics.minute AND
			( ( (acc.dir = 0) AND
				(acc.seg >= statistics.seg) AND
				(acc.seg <= statistics.seg + 4) 
			  ) OR
			  ( (acc.dir <> 0) AND
				(acc.seg <= statistics.seg) AND
				(acc.seg >= statistics.seg - 4)
			  )
			)
	);
*/

UPDATE statistics
SET     toll = 0,
		accident = 1
WHERE EXISTS(
	SELECT  acc.seg
	FROM    accident AS acc 
	WHERE   acc.dir = statistics.dir AND
			acc.firstMinute + 1 <= statistics.minute AND
			acc.lastMinute + 1 >= statistics.minute AND
			( ( (acc.dir = 0) AND
				(acc.seg >= statistics.seg) AND
				(acc.seg <= statistics.seg + 4) 
			  ) OR
			  ( (acc.dir <> 0) AND
				(acc.seg <= statistics.seg) AND
				(acc.seg >= statistics.seg - 4)
			  )
			)
	);

UPDATE statistics
SET    toll = (2 * (numVehicles - 50) * (numVehicles - 50))
WHERE  toll IS NULL;

/*
UPDATE statistics
SET accidentSeg = (
	SELECT  acc.seg
	FROM    accident AS acc 
	WHERE   acc.xway = statistics.xway AND
			acc.dir = statistics.dir AND
			acc.firstMinute + 1 <= statistics.minute AND
			acc.lastMinute + 1 >= statistics.minute AND
			( ( (acc.dir = 0) AND
				(acc.seg >= statistics.seg) AND
				(acc.seg <= statistics.seg + 4) 
			  ) OR
			  ( (acc.dir <> 0) AND
				(acc.seg <= statistics.seg) AND
				(acc.seg >= statistics.seg - 4)
			  )
			)
	)
WHERE statistics.accident = 1;
*/

UPDATE statistics
SET    accidentSeg = (
	SELECT  acc.seg
	FROM    accident AS acc 
	WHERE   acc.dir = statistics.dir AND
			acc.firstMinute + 1 <= statistics.minute AND
			acc.lastMinute + 1 >= statistics.minute AND
			( ( (acc.dir = 0) AND
				(acc.seg >= statistics.seg) AND
				(acc.seg <= statistics.seg + 4) 
			  ) OR
			  ( (acc.dir <> 0) AND
				(acc.seg <= statistics.seg) AND
				(acc.seg >= statistics.seg - 4)
			  )
			)
	)
WHERE statistics.accident = 1;

UPDATE  statistics
SET     accident = 0,
		accidentSeg = -1
WHERE  accident IS NULL;

----------------------------
--
-- createAlerts.pl

-- TRUNCATE TABLE tollAccAlerts;

/*
INSERT INTO tollAccAlerts(time, carid, xway, dir, seg)
SELECT  min(time), carid,
		xway, dir, seg
FROM    input
WHERE   type = 0 AND
lane <> 4
GROUP BY xway, dir, seg, carid;
*/

INSERT INTO tollAccAlerts(time, carid, dir, seg)
SELECT  min(time), carid,
		dir, seg
FROM    input
WHERE   type = 0 AND
		lane <> 4
GROUP BY dir, seg, carid;

/*
UPDATE tollAccAlerts
SET lav =
	(SELECT statistics.lav
	 FROM   statistics
	 WHERE  statistics.xway = tollAccAlerts.xway AND
			statistics.dir = tollAccAlerts.dir AND
			statistics.seg = tollAccAlerts.seg AND
			statistics.minute = trunc(tollAccAlerts.time/60) + 1 );
*/

UPDATE tollAccAlerts
SET lav =
	(SELECT statistics.lav
	 FROM   statistics
	 WHERE  statistics.dir = tollAccAlerts.dir AND
			statistics.seg = tollAccAlerts.seg AND
			statistics.minute = trunc(tollAccAlerts.time/60) + 1 );

/*
UPDATE tollAccAlerts
SET toll =
	(SELECT statistics.toll
	 FROM   statistics
	 WHERE  statistics.xway = tollAccAlerts.xway AND
			statistics.dir = tollAccAlerts.dir AND
			statistics.seg = tollAccAlerts.seg AND
			statistics.minute = trunc(tollAccAlerts.time/60) + 1 );
*/

UPDATE tollAccAlerts
SET toll =
	(SELECT statistics.toll
	 FROM   statistics
	 WHERE  statistics.dir = tollAccAlerts.dir AND
			statistics.seg = tollAccAlerts.seg AND
			statistics.minute = trunc(tollAccAlerts.time/60) + 1 );

/*
UPDATE tollAccAlerts
SET accidentSeg =
	(SELECT statistics.accidentSeg
	 FROM   statistics
	 WHERE  statistics.xway = tollAccAlerts.xway AND
			statistics.dir = tollAccAlerts.dir AND
			statistics.seg = tollAccAlerts.seg AND
			statistics.minute = trunc(tollAccAlerts.time/60) + 1 );
*/

UPDATE tollAccAlerts
SET accidentSeg =
	(SELECT statistics.accidentSeg
	 FROM   statistics
	 WHERE  statistics.dir = tollAccAlerts.dir AND
			statistics.seg = tollAccAlerts.seg AND
			statistics.minute = trunc(tollAccAlerts.time/60) + 1 );

			
-- for more than one xway, accumulate alerts.
/*
INSERT INTO tollAccAlertsTmp SELECT * from tollAccAlerts;
*/

-- end xway loop

-- for more than one xway, rename complete input and accumulated tolls
/*
DROP TABLE tollAccAlerts;
ALTER TABLE tollAccAlertsTmp RENAME TO tollAccAlerts;  
*/


------------------------------
--
-- splitbytype.pl

-- Split into a table of dailyExpenditureRequests

DROP TABLE IF EXISTS dailyExpenditureRequests;

-- Extract type 3 requests from input.
SELECT type, time, carid, xway, qid, day
	INTO dailyExpenditureRequests
	FROM input WHERE type=3;

-- Split into a table of accountBalanceRequest

DROP TABLE IF EXISTS accountBalanceRequests;

-- Extract type 2 requests from input.
SELECT type, time, carid as carid, xway, qid, day
	INTO accountBalanceRequests
	FROM input WHERE type=2;

------------------------------
--
-- accountBalanceAnswer.pl

-- Calculate type 2 answer.
-- Generate table accountBalancetimeEq with times for carids with
-- tollAccAlerts at same time as type 2 request. (resulttime=querytime)
SELECT t2.carid, t2.qid,  t2.time as querytime, t2.time as resulttime
	INTO  accountBalancetimeEq
	FROM  accountBalanceRequests as t2, tollAccAlerts
	WHERE t2.carid=tollAccAlerts.carid and t2.time = tollAccAlerts.time
	GROUP BY t2.carid, t2.time, t2.qid ORDER BY t2.carid, t2.time;


-- Use table accountBalancetimeEq to generate balances for carids with tollAccAlerts at same time as type 2 requests.
SELECT t2.carid, t2.querytime, t2.resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll
	INTO     accountBalanceNow
	FROM     accountBalancetimeEq as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid and
			 t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid
	ORDER BY t2.carid, t2.querytime;

-- Find max times of a tollalert for all carids having querytime>=resulttime1.
SELECT t2.carid, t2.qid, t2.time as querytime, max (tollAccAlerts.time) as resulttime
	INTO     accountBalanceTime0
	FROM     accountBalanceRequests as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid and
			 t2.time >=tollAccAlerts.time and
			 t2.time-30<tollAccAlerts.time
	GROUP BY t2.carid, t2.time, t2.qid
	ORDER BY t2.carid, t2.time;

-- Calculate maximum times where querytime>resulttime1 (second possible answer time).
SELECT t2.carid, t2.qid, t2.querytime, max(tollAccAlerts.time) AS resulttime
	INTO    accountBalanceTime1
	FROM    accountBalanceTime0 as t2, tollAccAlerts
	WHERE   t2.carid=tollAccAlerts.carid and
			t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid
	ORDER BY t2.carid, t2.resulttime;

-- Compensate for cars having t-30 as their last tollalert.
SELECT  t2.carid, t2.querytime as querytime, t2.resulttime as resulttime,
		t2.qid, sum(tollAccAlerts.toll) AS toll
	INTO     accountBalanceMiddle
	FROM     accountBalanceTime1 as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid and
			 t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;

-- Calculating possible resulttime2 thats different from querytime
-- for all carids having resulttime2<Resulttime1<querytime.
SELECT  t2.carid, t2.qid, t2.time as querytime,
		min (tollAccAlerts.time) as resulttime
	INTO     accountBalanceTime10
	FROM     accountBalanceRequests as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid and
			 t2.time >=tollAccAlerts.time and
			 t2.time-60<tollAccAlerts.time
	GROUP BY t2.carid, t2.time, t2.qid
	ORDER BY t2.carid, t2.time;

-- Calculate balances for resulttime2.
SELECT  t2.carid, t2.querytime, max(tollAccAlerts.time) AS resulttime,
		t2.qid, integer '0' as toll
	INTO    accountBalanceTime2
	FROM    accountBalanceTime10 as t2, tollAccAlerts
	WHERE   t2.carid=tollAccAlerts.carid and
			t2.resulttime >= tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid
	ORDER BY t2.carid, t2.resulttime;

-- Compensate for cars having t-60 as their last tollalert.
SELECT  t2.carid, t2.querytime as querytime,
		t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll
	INTO     accountBalanceLast
	FROM     accountBalanceTime2 as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid and
			 t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;

-- Calculate max times for cars not having any tollAccAlerts in the
-- 60 second window prior to a type 2 request.
SELECT  t2.carid, t2.time as querytime,
		max(tollAccAlerts.time) as resulttime, t2.qid, integer '0' as toll
	INTO     accountBalanceTime60
	FROM     accountBalanceRequests as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid and
			 t2.time-60 >= tollAccAlerts.time
	GROUP BY t2.carid, t2.time, t2.qid
	ORDER BY t2.carid, t2.time;


-- Delete from above calculation any cars already appearing in answer
DELETE FROM accountBalanceTime60
WHERE accountBalanceTime60.qid=accountBalanceTime0.qid;

DELETE FROM accountBalanceTime60
WHERE accountBalanceTime60.qid=accountBalanceTime10.qid;

DELETE FROM accountBalanceTime60
WHERE accountBalanceTime60.qid=accountBalanceTime2.qid;

-- Calculate tolls for the times where carids do not have tollAccAlerts
-- in the 60 second window prior to a type 2 request.
SELECT  t2.carid, t2.querytime as querytime, t2.resulttime as resulttime,
		t2.qid, sum(tollAccAlerts.toll) AS toll
	INTO     accountBalanceAncient
	FROM     accountBalanceTime60 as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid and
			 t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;

-- Calculating queries that have no tollAccAlerts.
DELETE FROM accountBalanceTime60
WHERE accountBalanceAncient.qid=accountBalanceTime60.qid;

-- Setting balance to zero for carids that have a type 2 request
-- but no previous tollAccAlerts.
SELECT  t2.carid, t2.time as querytime, min (tollAccAlerts.time) as resulttime,
		t2.qid, integer '0' AS toll
	INTO     accountBalanceQueryAtEntrance
	FROM     accountBalanceRequests as t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid
	GROUP BY t2.carid, t2.time, t2.qid
	ORDER BY t2.carid, t2.time;

DELETE FROM accountBalanceQueryAtEntrance
WHERE querytime<>resulttime;

DELETE FROM accountBalanceTime2
WHERE accountBalancelast.qid=accountBalanceTime2.qid;

-- Create answer in accountBalanceanswer table.
SELECT * into accountBalanceAnswer from accountBalanceNow
UNION SELECT * FROM accountBalanceMiddle
UNION SELECT * FROM accountBalanceLast
UNION SELECT * FROM accountBalanceAncient
UNION SELECT * FROM accountBalanceQueryatEntrance
UNION SELECT * FROM accountBalanceTime2
UNION SELECT * FROM accountBalanceTime60;


------------------------------
--
-- dailyExpenditureAnswer.pl

-- Calculate dailyExpenditureAnswer.

-- Summing query on relevant account balances.
SELECT  completeHistory.carid AS carid,
		completeHistory.day AS day,
		completeHistory.toll AS bal,
		dailyExpenditureRequests.qid
	INTO dailyExpenditureAnswer
	FROM dailyExpenditureRequests INNER JOIN completeHistory
	ON  (dailyExpenditureRequests.day = completeHistory.day)
	AND (dailyExpenditureRequests.carid = completeHistory.carid)
	AND (dailyExpenditureRequests.xway = completeHistory.xway);
