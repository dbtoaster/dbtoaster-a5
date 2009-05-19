-- Note: we only compute the bids vwap here. This code can
-- be replicated to compute the asks vwap if desired.

BEGIN TRANSACTION;

-- clean up triggers on base and internal tables.
IF OBJECT_ID(N'maintain_vwap_bcv_i', 'TR') IS NOT NULL
	DROP TRIGGER maintain_vwap_bcv_i;

IF OBJECT_ID(N'maintain_vwap_bcv_d', 'TR') IS NOT NULL
	DROP TRIGGER maintain_vwap_bcv_d;

IF OBJECT_ID(N'maintain_bcv_b2_i', 'TR') IS NOT NULL
	DROP TRIGGER maintain_bcv_b2_i;

IF OBJECT_ID(N'maintain_bcv_b2_d', 'TR') IS NOT NULL
	DROP TRIGGER maintain_bcv_b2_d;

IF OBJECT_ID(N'maintain_bcv_b1_i', 'TR') IS NOT NULL
	DROP TRIGGER maintain_bcv_b1_i;

IF OBJECT_ID(N'maintain_bcv_b1_d', 'TR') IS NOT NULL
	DROP TRIGGER maintain_bcv_b1_d;

IF OBJECT_ID(N'maintain_b2_i', 'TR') IS NOT NULL
	DROP TRIGGER maintain_b2_i;

IF OBJECT_ID(N'maintain_b2_d', 'TR') IS NOT NULL
	DROP TRIGGER maintain_b2_d;

IF OBJECT_ID(N'maintain_bv_i', 'TR') IS NOT NULL
	DROP TRIGGER maintain_bv_i;

IF OBJECT_ID(N'maintain_bv_d', 'TR') IS NOT NULL
	DROP TRIGGER maintain_bv_d;


-- clean up and recreate original base tables
IF OBJECT_ID(N'bids', 'table') IS NOT NULL
	DROP TABLE bids;

CREATE TABLE bids (ts bigint, id bigint, price decimal, volume decimal);

IF OBJECT_ID(N'asks', 'table') IS NOT NULL
	DROP TABLE asks;

CREATE TABLE asks (ts bigint, id bigint, price decimal, volume decimal);


-- clean up and recreate internal tables
IF OBJECT_ID(N'bv', 'table') IS NOT NULL
	DROP TABLE bv;

CREATE TABLE bv (total_volume decimal);

IF OBJECT_ID(N'b2', 'table') IS NOT NULL
	DROP TABLE b2;

CREATE TABLE b2 (price decimal);

IF OBJECT_ID(N'bcv', 'table') IS NOT NULL
	DROP TABLE bcv;

CREATE TABLE bcv (price decimal, cumsum_volume decimal);

IF OBJECT_ID(N'vwap', 'table') IS NOT NULL
	DROP TABLE vwap;

CREATE TABLE vwap (price decimal, volume decimal);


COMMIT TRANSACTION;
GO


-- bv triggers
CREATE TRIGGER maintain_bv_i ON bids AFTER INSERT, UPDATE AS
	UPDATE bv SET total_volume = total_volume + volume
	FROM inserted;
GO

CREATE TRIGGER maintain_bv_d ON bids AFTER DELETE, UPDATE AS
	UPDATE bv SET total_volume = total_volume - volume
	FROM deleted;
GO

-- b2 triggers
-- assumes no price changes on updates to bids, i.e. price
-- changes are explicit inserts/updates
CREATE TRIGGER maintain_b2_i ON bids AFTER INSERT AS
	INSERT INTO b2
		SELECT price FROM inserted
		WHERE price NOT IN (SELECT price from b2)
GO

CREATE TRIGGER maintain_b2_d ON bids AFTER DELETE AS
	DELETE FROM b2
	WHERE price IN
		(SELECT price FROM deleted
		WHERE price NOT IN (SELECT price from bids))
GO

-- bcv via b1 triggers
CREATE TRIGGER maintain_bcv_b1_i ON bids AFTER INSERT, UPDATE AS
	UPDATE bcv
		SET cumsum_volume = cumsum_volume + delta.volume
		FROM
			(SELECT b2.price, inserted.volume
			 FROM b2, inserted
			 WHERE b2.price < inserted.price)
			AS delta
		WHERE bcv.price = delta.price
GO
		
CREATE TRIGGER maintain_bcv_b1_d ON bids AFTER DELETE, UPDATE AS
	UPDATE bcv
		SET cumsum_volume = cumsum_volume - delta.volume
		FROM
			(SELECT b2.price, deleted.volume
			 FROM b2, deleted
			 WHERE b2.price < deleted.price)
			AS delta
		WHERE bcv.price = delta.price
GO

-- bcv via b2 triggers
CREATE TRIGGER maintain_bcv_b2_i ON b2 AFTER INSERT AS
    INSERT INTO bcv (price, cumsum_volume)
    SELECT inserted.price, sum(bids.volume) AS cumsum_volume
        FROM bids, inserted
        WHERE bids.price > inserted.price
        GROUP BY inserted.price
GO

CREATE TRIGGER maintain_bcv_b2_d ON b2 AFTER DELETE AS
	DELETE FROM bcv WHERE price IN (SELECT price from deleted)
GO

-- vwap via bcv
CREATE TRIGGER maintain_vwap_bcv_i ON bcv AFTER INSERT, UPDATE AS
	DECLARE @vol_limit decimal
	SELECT @vol_limit = 0.25*total_volume FROM bv
	INSERT INTO vwap
		SELECT bids.price, bids.volume
		FROM bids, inserted
		WHERE bids.price = inserted.price
		AND inserted.cumsum_volume < @vol_limit
GO

CREATE TRIGGER maintain_vwap_bcv_d ON bcv AFTER DELETE, UPDATE AS
	DELETE FROM vwap WHERE price IN (SELECT price from deleted)
GO