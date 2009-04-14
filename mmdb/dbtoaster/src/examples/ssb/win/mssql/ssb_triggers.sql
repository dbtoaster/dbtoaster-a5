BEGIN TRANSACTION;

IF OBJECT_ID(N'lineitem', 'table') IS NOT NULL
	DROP TABLE lineitem;

IF OBJECT_ID(N'orders', 'table') IS NOT NULL
	DROP TABLE orders;

IF OBJECT_ID(N'part', 'table') IS NOT NULL
	DROP TABLE part;

IF OBJECT_ID(N'customer', 'table') IS NOT NULL
	DROP TABLE customer;

IF OBJECT_ID(N'supplier', 'table') IS NOT NULL
	DROP TABLE supplier;

IF OBJECT_ID(N'region', 'table') IS NOT NULL
	DROP TABLE region;

IF OBJECT_ID(N'nation', 'table') IS NOT NULL
	DROP TABLE nation;


CREATE TABLE lineitem (
	orderkey bigint, partkey bigint, suppkey bigint,
	linenumber integer,
	quantity decimal, extendedprice decimal,
	discount decimal, tax decimal,
	returnflag char(1), linestatus char(1),
	shipdate date, commitdate date, receiptdate date,
	shipinstruct char(25), shipmode char(10),
	comment varchar(44),
	primary key (orderkey, linenumber));

CREATE TABLE orders (
	orderkey bigint, custkey bigint,
	orderstatus char(1), totalprice decimal,
	orderdate date, orderpriority char(15),
	clerk char(15), shippriority integer,
	comment varchar(79),
	primary key (orderkey));

CREATE TABLE part (
	partkey bigint, name varchar(55),
	mfgr char(25), brand char(10),
	type varchar(25), size integer,
	container char(10), retailprice decimal,
	comment varchar(25),
	primary key (partkey));

CREATE TABLE customer (
	custkey bigint, name varchar(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	mktsegment char(10), comment varchar(117),
	primary key (custkey));

CREATE TABLE supplier (
	suppkey bigint, name char(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	comment varchar(101),
	primary key (suppkey));

CREATE TABLE region (
	regionkey bigint, name char(25), comment varchar(125),
	primary key (regionkey));

CREATE TABLE nation (
	nationkey bigint, name char(25),
	regionkey bigint, comment varchar(152),
	primary key (nationkey));
	
COMMIT TRANSACTION;

BEGIN TRANSACTION;

-- TODO: drop triggers

-- clean up internal tables
IF OBJECT_ID(N'ssb_date', 'table') IS NOT NULL
	DROP TABLE ssb_date;

IF OBJECT_ID(N'ssb_customer', 'table') IS NOT NULL
	DROP TABLE ssb_customer;

IF OBJECT_ID(N'ssb_supplier', 'table') IS NOT NULL
	DROP TABLE ssb_supplier;

IF OBJECT_ID(N'ssb_part', 'table') IS NOT NULL
	DROP TABLE ssb_part;

IF OBJECT_ID(N'ssb_totalprices', 'table') IS NOT NULL
	DROP TABLE ssb_totalprices;

IF OBJECT_ID(N'ssb_lineorder', 'table') IS NOT NULL
	DROP TABLE ssb_lineorder;

IF OBJECT_ID(N'query_result', 'table') IS NOT NULL
	DROP TABLE query_result;


CREATE TABLE ssb_date (datekey date, year double precision);

CREATE TABLE ssb_customer (
	custkey bigint,
	name varchar(25),
	address varchar(40),
	city text,
	nation char(25),
	region char(25),
	phone char(15),
	mktsegment char(10)
);

CREATE TABLE ssb_supplier (
	suppkey bigint,
	name varchar(25),
	address varchar(40),
	city text,
	nation char(25),
	region char(25),
	phone char(15)
);

CREATE TABLE ssb_part (
	partkey bigint,
	name varchar(55),
	mfgr char(25),
	category char(10),
	brand1 text,
	color text,
	type varchar(25),
	size integer,
	container char(10)
);

CREATE TABLE ssb_totalprices (
	orderkey bigint,
	totalprice decimal
);

CREATE TABLE ssb_lineorder (
	orderkey bigint,
	linenumber integer,
	custkey bigint,
	partkey bigint,
	suppkey bigint,
	orderdate date,
	orderpriority char(15),
	shippriority integer,
	quantity decimal,
	extendedprice decimal,
	ordtotalprice decimal,
	discount decimal,
	revenue decimal,
	supplycost decimal,
	tax decimal,
	commitdate date,
	shipmode char(10)
);

CREATE TABLE query_result (
	year double precision,
	nation char(25),
	profit decimal,
	refcount integer
);

COMMIT TRANSACTION;
GO

-- ssb_date via orders
CREATE TRIGGER maintain_ssb_date_orders_i ON orders AFTER INSERT AS
	INSERT INTO ssb_date
		SELECT inserted.orderdate, year(inserted.orderdate) FROM inserted
		WHERE inserted.orderdate NOT IN (select datekey FROM ssb_date)
GO

CREATE TRIGGER maintain_ssb_date_orders_d ON orders AFTER DELETE AS
	DELETE FROM ssb_date
	WHERE datekey IN
		(SELECT orderdate FROM deleted
		WHERE orderdate NOT IN (SELECT orderdate FROM orders))
GO

-- ssb_customers via customers
CREATE TRIGGER maintain_ssb_customer_customer_i ON customer AFTER INSERT AS
	INSERT INTO ssb_customer
	SELECT inserted.custkey, inserted.name, inserted.address,
			(substring(nation.name, 0, 9) + str(round(rand() * 9, 0))) as city,
			nation.name as nation_name, region.name as region_name,
			inserted.phone, inserted.mktsegment
	FROM nation, inserted, region
	WHERE nation.nationkey = inserted.nationkey
	AND region.regionkey = nation.regionkey
GO

CREATE TRIGGER maintain_ssb_customer_customer_d ON customer AFTER DELETE AS
	DELETE FROM ssb_customer WHERE custkey IN (SELECT custkey FROM deleted)
GO

-- ssb_supplier via supplier
CREATE TRIGGER maintain_ssb_supplier_supplier_i ON supplier AFTER INSERT AS
	INSERT INTO ssb_supplier
	SELECT inserted.suppkey, inserted.name, inserted.address
			(substring(nation.name, 0, 9) + str(round(rand() * 9, 0))) as city,
			nation.name, region.name, inserted.phone
	FROM inserted, nation, region
	WHERE inserted.nationkey = nation.nationkey
	AND nation.regionkey = region.regionkey
GO

CREATE TRIGGER maintain_ssb_supplier_supplier_d ON supplier AFTER DELETE AS
	DELETE FROM ssb_supplier WHERE suppkey IN (SELECT suppkey FROM deleted)
GO

-- ssb_part via part
CREATE TRIGGER maintain_ssb_part_part_i ON part AFTER INSERT AS
	INSERT INTO ssb_part
	SELECT inserted.partkey, inserted.name, inserted.mfgr, inserted.brand,
		(inserted.brand + str(round(rand() * 25, 0))),
		substring(inserted.name, 0, charindex(' ', inserted.name)),
		inserted.type, inserted.size, inserted.container
	FROM inserted
GO

CREATE TRIGGER maintain_ssb_part_part_d ON part AFTER DELETE AS
	DELETE FROM ssb_part WHERE partkey IN (SELECT partkey FROM deleted)
GO

-- ssb_totalprices via lineitem
CREATE TRIGGER maintain_ssb_totalprices_lineitem_i ON lineitem AFTER INSERT AS
	BEGIN
		INSERT INTO ssb_totalprices
		SELECT inserted.orderkey,
			(inserted.extendedprice*(100-inserted.discount)*(100+inserted.tax))/100
		FROM inserted
		WHERE inserted.orderkey NOT IN (SELECT orderkey FROM ssb_totalprices)

		UPDATE ssb_totalprices
		SET totalprice = totalprice + 
			(extendedprice*(100-discount)*(100+tax))/100
		FROM inserted
		WHERE inserted.orderkey IN (SELECT orderkey FROM ssb_totalprices)
	END;
GO

CREATE TRIGGER maintain_ssb_totalprices_lineitem_d ON lineitem AFTER DELETE AS
	BEGIN
		DELETE FROM ssb_totalprices WHERE orderkey IN
			(SELECT orderkey FROM deleted
			WHERE orderkey NOT IN (SELECT orderkey FROM lineitem))

		UPDATE ssb_totalprices
		SET totalprice = totalprice -
			(extendedprice*(100-discount)*(100+tax))/100
		FROM deleted
		WHERE deleted.orderkey IN (SELECT orderkey FROM lineitem)
	END;
GO

-- ssb_lineorder via lineitem
CREATE TRIGGER maintain_ssb_lineorder_lineitem_i ON lineitem AFTER INSERT AS
	INSERT INTO ssb_lineorder
	SELECT inserted.orderkey, inserted.linenumber,
		orders.custkey, inserted.partkey, inserted.suppkey,
		orders.orderdate, orders.orderpriority, orders.shippriority,
		inserted.quantity, inserted.extendedprice,
		ssb_totalprices.totalprice, inserted.discount,
		(inserted.extendedprice * (100 - inserted.discount)/100),
		(90000 + (((inserted.partkey/10) % 20001) + ((inserted.partkey % 1000)*100))),
		inserted.tax, inserted.commitdate, inserted.shipmode
	FROM inserted, orders, ssb_totalprices
	WHERE inserted.orderkey = orders.orderkey
	AND inserted.orderkey = ssb_totalprices.orderkey
GO

CREATE TRIGGER maintain_ssb_lineorder_lineitem_d ON lineitem AFTER DELETE AS
	DELETE FROM ssb_lineorder
		FROM deleted
		WHERE ssb_lineorder.orderkey = deleted.orderkey
		AND ssb_lineorder.linenumber = deleted.linenumber
GO

-- ssb_lineorder from orders
CREATE TRIGGER maintain_ssb_lineorder_orders_i ON orders AFTER INSERT AS
	INSERT INTO ssb_lineorder
	SELECT inserted.orderkey, lineitem.linenumber,
		inserted.custkey, lineitem.partkey, lineitem.suppkey,
		inserted.orderdate, inserted.orderpriority,
		inserted.shippriority, lineitem.quantity, lineitem.extendedprice,
		ssb_totalprices.totalprice, lineitem.discount,
		(lineitem.extendedprice * (100-lineitem.discount)/100) as revenue,
		(90000 + ((lineitem.partkey/10) % 20001) + ((lineitem.partkey % 1000)*100)) as supplycost,
		lineitem.tax, lineitem.commitdate, lineitem.shipmode
	FROM inserted, lineitem, ssb_totalprices
	WHERE inserted.orderkey = lineitem.orderkey
	AND inserted.orderkey = ssb_totalprices.orderkey
GO

CREATE TRIGGER maintain_ssb_lineorder_orders_d ON orders AFTER DELETE AS
	DELETE FROM ssb_lineorder
	WHERE orderkey IN (SELECT orderkey FROM deleted)
GO


-- query via lineorder
CREATE TRIGGER maintain_result_lineorder_i ON ssb_lineorder AFTER INSERT AS
	DECLARE @new_results TABLE(
		new_year double precision,
		new_nation char(25),
		new_profit decimal,
		new_refcount integer)

	INSERT INTO @new_results
	SELECT inserted_results.year as new_year, inserted_results.nation as new_nation,
		inserted_results.profit as new_profit, query_results.refcount
	FROM
		(SELECT d.year, c.nation, (inserted.revenue - inserted.supplycost) as profit
		FROM inserted, ssb_date AS d, ssb_customer AS c, ssb_supplier AS s, ssb_part AS p
		WHERE inserted.custkey = c.custkey
		AND inserted.partkey = p.partkey
		AND inserted.orderdate = d.datekey
		AND c.region = 'AMERICA'
		AND s.region = 'AMERICA'
		AND (p.mfgr = 'Manufacturer#1' or p.mfgr = 'Manufacturer#2'))
	AS inserted_results
	LEFT OUTER JOIN query_results
	ON (inserted_results.year = query_results.year
		AND inserted_results.nation = query_results.nation)

	INSERT INTO query_result
	SELECT new_year, new_nation, new_profit, 1 as refcount
	FROM @new_results WHERE new_refcount = NULL
	
	UPDATE query_result
		SET query_result.profit = query_result.profit + new_profit,
			query_result.refcount = query_result.refcount + 1
		FROM @new_results
		WHERE query_result.year = new_year
		AND query_result.nation = new_nation
GO

CREATE TRIGGER maintain_result_lineorder_d ON ssb_lineorder AFTER DELETE AS
	DECLARE @old_results TABLE(
		old_year double precision,
		old_nation char(25),
		old_profit decimal,
		old_refcount integer)

	INSERT INTO @old_results
	SELECT deleted_results.year as old_year, deleted_results.nation as old_nation,
		(deleted_results.revenue - deleted_results.supplycost) AS old_profit, query_results.refcount
	FROM
		(SELECT d.year, c.nation, (deleted.revenue - deleted.supplycost) as profit
		FROM deleted, ssb_date AS d, ssb_customer AS c, ssb_supplier AS s, ssb_part AS p
		WHERE deleted.custkey = c.custkey
		AND deleted.partkey = p.partkey
		AND deleted.orderdate = d.datekey
		AND c.region = 'AMERICA'
		AND s.region = 'AMERICA'
		AND (p.mfgr = 'Manufacturer#1' or p.mfgr = 'Manufacturer#2'))
	AS deleted_results
	JOIN query_results
	ON (deleted_results.year = query_results.year
		AND deleted_results.nation = query_results.nation)
		
	DELETE FROM query_result
		FROM @old_results
		WHERE old_refcount = 1
		AND query_result.year = old_year
		AND query_result.nation = old_nation

	UPDATE query_result
		SET query_result.profit = query_result.profit - old_profit,
			query_result.refcount = query_result.refcount - 1
		FROM @old_results
		WHERE query_result.year = old_year
		AND query_result.nation = old_nation
GO

-- run...
/*
BULK INSERT region FROM '/home/yanif/datasets/tpch/sf1/singlefile/region.tbl'
WITH ( FIELDTERMINATOR='|', ROWTERMINATOR='\n' );

BULK INSERT nation FROM '/home/yanif/datasets/tpch/sf1/singlefile/nation.tbl'
WITH ( FIELDTERMINATOR='|', ROWTERMINATOR='\n' );

BULK INSERT part FROM '/home/yanif/datasets/tpch/sf1/singlefile/part.tbl.a'
WITH ( FIELDTERMINATOR='|', ROWTERMINATOR='\n', FIRE_TRIGGERS );

BULK INSERT customer FROM '/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a'
WITH ( FIELDTERMINATOR='|', ROWTERMINATOR='\n', FIRE_TRIGGERS );

BULK INSERT supplier FROM '/home/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a'
WITH ( FIELDTERMINATOR='|', ROWTERMINATOR='\n', FIRE_TRIGGERS );
*/