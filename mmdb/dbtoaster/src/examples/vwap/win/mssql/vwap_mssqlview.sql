IF OBJECT_ID(N'bids_vwap_idx', 'index') IS NOT NULL
	DROP INDEX bids_vwap_idx ON bids_vwap;
GO

IF OBJECT_ID(N'bids_vwap', 'view') IS NOT NULL
	DROP VIEW bids_vwap;
GO

IF OBJECT_ID(N'bids', 'table') IS NOT NULL
	DROP TABLE bids;

CREATE TABLE bids (ts bigint, id bigint, price decimal, volume decimal);

IF OBJECT_ID(N'asks', 'table') IS NOT NULL
	DROP TABLE asks;

CREATE TABLE asks (ts bigint, id bigint, price decimal, volume decimal);
GO

CREATE VIEW bids_vwap WITH SCHEMABINDING AS
SELECT b3.id, b3.price, b3.volume FROM
	(SELECT SUM(volume) AS total_volume FROM dbo.bids) AS bv,
	(SELECT b2.price, SUM(b1.volume) AS cumsum_volume
		FROM dbo.bids b1 RIGHT OUTER JOIN
			(SELECT DISTINCT price FROM dbo.bids) AS b2
		ON (b1.price > b2.price)
		GROUP BY b2.price) AS bcv,
	dbo.bids AS b3
	WHERE 0.25 * bv.total_volume > bcv.cumsum_volume
	AND bcv.price = b3.price;
GO

CREATE UNIQUE CLUSTERED INDEX bids_vwap_idx ON bids_vwap(id, price, volume);
GO