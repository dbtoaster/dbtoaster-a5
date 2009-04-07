BEGIN TRANSACTION;
drop table if exists lineitem;
drop table if exists orders;
drop table if exists part;
drop table if exists customer;
drop table if exists supplier;
drop table if exists region;
drop table if exists nation;


create table lineitem (
	orderkey bigint, partkey bigint, suppkey bigint,
	linenumber integer,
	quantity decimal, extendedprice decimal,
	discount decimal, tax decimal,
	returnflag char(1), linestatus char(1),
	shipdate date, commitdate date, receiptdate date,
	shipinstruct char(25), shipmode char(10),
	comment varchar(44),
	primary key (orderkey, linenumber));

create table orders (
	orderkey bigint, custkey bigint,
	orderstatus char(1), totalprice decimal,
	orderdate date, orderpriority char(15),
	clerk char(15), shippriority integer,
	comment varchar(79),
	primary key (orderkey));

create table part (
	partkey bigint, name varchar(55),
	mfgr char(25), brand char(10),
	type varchar(25), size integer,
	container char(10), retailprice decimal,
	comment varchar(25),
	primary key (partkey));

create table customer (
	custkey bigint, name varchar(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	mktsegment char(10), comment varchar(117),
	primary key (custkey));

create table supplier (
	suppkey bigint, name char(25),
	address varchar(40), nationkey bigint,
	phone char(15), acctbal decimal,
	comment varchar(101),
	primary key (suppkey));

create table region (
	regionkey bigint, name char(25), comment varchar(125),
	primary key (regionkey));

create table nation (
	nationkey bigint, name char(25),
	regionkey bigint, comment varchar(152),
	primary key (nationkey));

END TRANSACTION;

BEGIN TRANSACTION;

DROP TRIGGER IF EXISTS maintain_ssb_lineorder_orders ON orders;
DROP TRIGGER IF EXISTS maintain_ssb_lineorder_lineitem ON lineitem;
DROP TRIGGER IF EXISTS maintain_ssb_totalprices_lineitem ON lineitem;
DROP TRIGGER IF EXISTS maintain_ssb_part_part on part;
DROP TRIGGER IF EXISTS maintain_ssb_supplier_supplier ON supplier;
DROP TRIGGER IF EXISTS maintain_ssb_customer_customer ON customer;
DROP TRIGGER IF EXISTS maintain_ssb_date_orders ON orders; 

DROP TABLE IF EXISTS ssb_date;
DROP TABLE IF EXISTS ssb_customer;
DROP TABLE IF EXISTS ssb_supplier;
DROP TABLE IF EXISTS ssb_part;
DROP TABLE IF EXISTS ssb_totalprices;
DROP TABLE IF EXISTS ssb_lineorder;
DROP TABLE IF EXISTS query_result;

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

--
-- TODO: create index on ssb_date.datekey
-- TODO: ensure index on orders.orderdate
CREATE OR REPLACE FUNCTION maintain_ssb_date_orders()
RETURNS TRIGGER AS $maintain_ssb_date_orders$
	DECLARE
		date_count integer;
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			SELECT COUNT(*) INTO date_count FROM ssb_date WHERE datekey = NEW.orderdate;
			IF (date_count = 0) THEN
				INSERT INTO ssb_date VALUES(NEW.orderdate, extract(year from NEW.orderdate));
			END IF;

		ELSIF (TG_OP = 'DELETE') THEN
			SELECT COUNT(*) INTO date_count FROM orders WHERE orderdate = OLD.orderdate;
			IF (date_count = 0) THEN
				DELETE FROM ssb_date WHERE datekey = OLD.orderdate;
			END IF;

		END IF;
		RETURN NULL;
	END;
$maintain_ssb_date_orders$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_ssb_date_orders
	AFTER INSERT OR DELETE ON orders
	FOR EACH ROW EXECUTE PROCEDURE maintain_ssb_date_orders();

--
CREATE OR REPLACE FUNCTION maintain_ssb_customer_customer()
RETURNS TRIGGER AS $maintain_ssb_customer_customer$
	DECLARE
		city text;
		nation_rk bigint;
		nation_name varchar(25);
		region_name varchar(25);
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			SELECT name, regionkey INTO nation_name, nation_rk
				FROM nation WHERE nation.nationkey = NEW.nationkey;

			SELECT name INTO region_name FROM region WHERE regionkey = nation_rk;

			city := (substr(nation_name, 0, 9) || (cast (round(random() * 9) as text)));

			INSERT INTO ssb_customer VALUES (
				NEW.custkey, NEW.name, NEW.address,
				city, nation_name, region_name,
				NEW.phone, NEW.mktsegment);

		ELSIF (TG_OP = 'DELETE') THEN
			DELETE FROM ssb_customer WHERE custkey = OLD.custkey;

		END IF;
		RETURN NULL;
	END;
$maintain_ssb_customer_customer$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_ssb_customer_customer
	AFTER INSERT OR DELETE ON customer
	FOR EACH ROW EXECUTE PROCEDURE maintain_ssb_customer_customer();

--
CREATE OR REPLACE FUNCTION maintain_ssb_supplier_supplier()
RETURNS TRIGGER AS $maintain_ssb_supplier_supplier$
	DECLARE
		city text;
		nation_rk bigint;
		nation_name varchar(25);
		region_name varchar(25);
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			SELECT name, regionkey INTO nation_name, nation_rk
				FROM nation WHERE nation.nationkey = NEW.nationkey;

			SELECT name INTO region_name FROM region WHERE regionkey = nation_rk;

			city := (substr(nation_name, 0, 9) || (cast (round(random() * 9) as text)));

			INSERT INTO ssb_supplier VALUES (
				NEW.suppkey, NEW.name, NEW.address,
				city, nation_name, region_name, NEW.phone);
			
		ELSIF (TG_OP = 'DELETE') THEN
			DELETE FROM ssb_supplier WHERE suppkey = OLD.suppkey;

		END IF;
		RETURN NULL;
	END;
$maintain_ssb_supplier_supplier$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_ssb_supplier_supplier
	AFTER INSERT OR DELETE ON supplier
	FOR EACH ROW EXECUTE PROCEDURE maintain_ssb_supplier_supplier();

--
CREATE OR REPLACE FUNCTION maintain_ssb_part_part()
RETURNS TRIGGER AS $maintain_ssb_part$
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			INSERT INTO ssb_part VALUES (
				NEW.partkey, NEW.name, NEW.mfgr, NEW.brand,
			    (NEW.brand || (cast (round(random()*25) as text))),
    			substring(NEW.name from 0 for position(' ' in NEW.name)),
    			NEW.type, NEW.size, NEW.container);

		ELSIF (TG_OP = 'DELETE') THEN
			DELETE FROM ssb_part WHERE partkey = OLD.partkey;

		END IF;
		RETURN NULL;
	END;
$maintain_ssb_part$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_ssb_part_part
	AFTER INSERT OR DELETE on part
	FOR EACH ROW EXECUTE PROCEDURE maintain_ssb_part_part();

--
CREATE OR REPLACE FUNCTION maintain_ssb_totalprices_lineitem()
RETURNS TRIGGER AS $maintain_ssb_totalprices_lineitem$
	DECLARE
		line_count integer;
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			SELECT COUNT(*) INTO line_count FROM lineitem WHERE lineitem.orderkey = NEW.orderkey;
			IF (line_count = 1) THEN
				INSERT INTO ssb_totalprices VALUES (NEW.orderkey,
					(NEW.extendedprice*(100-NEW.discount)*(100+NEW.tax))/100);
			ELSE
				UPDATE ssb_totalprices
					SET totalprice = totalprice +
						(NEW.extendedprice*(100-NEW.discount)*(100+NEW.tax))/100
					WHERE ssb_totalprices.orderkey = NEW.orderkey; 
			END IF;

		ELSIF (TG_OP = 'DELETE') THEN
			SELECT COUNT(*) INTO line_count FROM lineitem WHERE lineitem.orderkey = OLD.orderkey;
			IF (line_count = 0) THEN
				DELETE FROM ssb_totalprices WHERE ssb_totalprices.orderkey = OLD.orderkey;
			ELSE
				UPDATE ssb_totalprices
					SET totalprice = totalprice -
						(OLD.extendedprice*(100-OLD.discount)*(100+OLD.tax))/100
					WHERE ssb_totalprices.orderkey = OLD.orderkey;
			END IF;
		END IF;
		RETURN NULL;
	END;
$maintain_ssb_totalprices_lineitem$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_ssb_totalprices_lineitem
	AFTER INSERT OR DELETE ON lineitem
	FOR EACH ROW EXECUTE PROCEDURE maintain_ssb_totalprices_lineitem();

--
CREATE OR REPLACE FUNCTION maintain_ssb_lineorder_lineitem()
RETURNS TRIGGER AS $maintain_ssb_lineorder_lineitem$
	DECLARE
		-- orders fields
		custkey bigint;
		orderdate date;
		orderpriority char(15);
		shippriority integer;
		
		-- ssb_totalprices fields
		ordtotalprice decimal;
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			SELECT orders.custkey, orders.orderdate,
					orders.orderpriority, orders.shippriority
				INTO custkey, orderdate, orderpriority, shippriority
				FROM orders
				WHERE orders.orderkey = NEW.orderkey;

			SELECT totalprice into ordtotalprice FROM ssb_totalprices
				WHERE ssb_totalprices.orderkey = NEW.orderkey;

			INSERT INTO ssb_lineorder VALUES (
				NEW.orderkey, NEW.linenumber,
				custkey, NEW.partkey, NEW.suppkey,
				orderdate, orderpriority, shippriority,
				NEW.quantity, NEW.extendedprice, ordtotalprice, NEW.discount,
				(NEW.extendedprice * (100-NEW.discount)/100), 
			    (90000 + ((NEW.partkey/10) % 20001) + ((NEW.partkey % 1000)*100)),
			    NEW.tax, NEW.commitdate, NEW.shipmode);

		ELSIF (TG_OP = 'DELETE') THEN
			DELETE FROM ssb_lineorder
				WHERE ssb_lineorder.orderkey = OLD.orderkey AND linenumber = OLD.linenumber;

		END IF;
		RETURN NULL;
	END;
$maintain_ssb_lineorder_lineitem$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_ssb_lineorder_lineitem
	AFTER INSERT OR DELETE ON lineitem
	FOR EACH ROW EXECUTE PROCEDURE maintain_ssb_lineorder_lineitem();

--
CREATE OR REPLACE FUNCTION maintain_ssb_lineorder_orders()
RETURNS TRIGGER AS $maintain_ssb_lineorder_orders$
	DECLARE
		-- lineitem fields
		linenumber integer;
		partkey bigint;
		suppkey bigint;
		quantity decimal;
		extendedprice decimal;
		ordtotalprice decimal;
		discount decimal;
		revenue decimal;
		supplycost decimal;
		tax decimal;
		commitdate date;
		shipmode char(10);

		-- ssb_totalprices fields
		totalprice decimal;
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			SELECT
					lineitem.linenumber, lineitem.partkey, lineitem.suppkey,
					lineitem.quantity, lineitem.extendedprice, lineitem.discount,
				    (lineitem.extendedprice * (100-lineitem.discount)/100),
				    (90000 + ((lineitem.partkey/10) % 20001) + ((lineitem.partkey % 1000)*100)),
					lineitem.tax, lineitem.commitdate, lineitem.shipmode
				INTO
					linenumber, partkey, suppkey,
					quantity, extendedprice, discount,
					revenue, supplycost,
					tax, commitdate, shipmode
				FROM lineitem
				WHERE lineitem.orderkey = NEW.orderkey;

			SELECT ssb_totalprices.totalprice INTO ordtotalprice
			FROM ssb_totalprices WHERE ssb_totalprices.orderkey = NEW.orderkey;

			INSERT INTO ssb_lineorder VALUES (
				NEW.orderkey, linenumber,
				NEW.custkey, partkey, suppkey,
				NEW.orderdate, NEW.orderpriority,
				NEW.shippriority, quantity, extendedprice,
				ordtotalprice, discount, revenue, supplycost,
				tax, commitdate, shipmode);

		ELSIF (TG_OP = 'DELETE') THEN
			DELETE FROM ssb_lineorder WHERE ssb_lineorder.orderkey = OLD.orderkey;

		END IF;
		RETURN NULL;
	END;
$maintain_ssb_lineorder_orders$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_ssb_lineorder_orders
	AFTER INSERT OR DELETE ON orders
	FOR EACH ROW EXECUTE PROCEDURE maintain_ssb_lineorder_orders();
	

-- query via lineorder
CREATE OR REPLACE FUNCTION maintain_result_lineorder()
RETURNS TRIGGER AS $maintain_result_lineorder$
	DECLARE
		qr_year double precision;
		qr_nation char(25);
		qr_profit decimal;
		qr_lo_count integer;
	BEGIN
		IF (TG_OP = 'INSERT') THEN
			SELECT d.year, c.nation, (NEW.revenue - NEW.supplycost)
				INTO qr_year, qr_nation, qr_profit  
			FROM ssb_date AS d, ssb_customer AS c, ssb_supplier AS s, ssb_part AS p
			WHERE c.custkey = NEW.custkey
			AND s.suppkey = NEW.suppkey
			AND p.partkey = NEW.partkey
			AND d.datekey = NEW.orderdate
			AND c.region = 'AMERICA'
			AND s.region = 'AMERICA'
			AND (p.mfgr = 'Manufacturer#1' or p.mfgr = 'Manufacturer#2');

			SELECT COUNT(*) INTO qr_lo_count
				FROM query_result
				WHERE year = qr_year
				AND nation = qr_nation;
				
			IF (qr_lo_count = 0) THEN
				INSERT INTO query_result VALUES (qr_year, qr_nation, qr_profit, 1);
			ELSE
				UPDATE query_result
					SET profit = profit + (NEW.revenue - NEW.supplycost),
						refcount = refcount + 1
				WHERE year = qr_year 
				AND nation = qr_nation;
			END IF; 

		ELSIF (TG_OP = 'DELETE') THEN
			SELECT d.year, c.nation, (OLD.revenue - OLD.supplycost)
				INTO qr_year, qr_nation, qr_profit  
			FROM ssb_date AS d, ssb_customer AS c, ssb_supplier AS s, ssb_part AS p
			WHERE c.custkey = OLD.custkey
			AND s.suppkey = OLD.suppkey
			AND p.partkey = OLD.partkey
			AND d.datekey = OLD.orderdate
			AND c.region = 'AMERICA'
			AND s.region = 'AMERICA'
			AND (p.mfgr = 'Manufacturer#1' or p.mfgr = 'Manufacturer#2');

			SELECT refcount INTO qr_lo_count
				FROM query_result
				WHERE year = qr_year
				AND nation = qr_nation;

			IF (qr_lo_count = 1) THEN
				DELETE FROM query_result
					WHERE year = qr_year
					AND nation = qr_nation;

			ELSE
				UPDATE query_result
					SET profit = profit - OLD.profit
					WHERE year = qr_year
					AND nation = qr_nation;

			END IF;
		END IF;
		RETURN NULL;
	END;
$maintain_result_lineorder$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_result_lineorder
	AFTER INSERT OR DELETE ON ssb_lineorder
	FOR EACH ROW EXECUTE PROCEDURE maintain_result_lineorder();

END TRANSACTION;

-- run...
copy region (regionkey, name, comment) from '/home/yanif/datasets/tpch/sf1/singlefile/region.tbl' with delimiter '|';

copy nation (nationkey, name, regionkey, comment) from '/home/yanif/datasets/tpch/sf1/singlefile/nation.tbl' with delimiter '|';

copy part from '/home/yanif/datasets/tpch/sf1/singlefile/part.tbl.a' with delimiter '|';
copy customer from '/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a' with delimiter '|';
copy supplier from '/home/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a' with delimiter '|';

copy orders from '/home/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a' with delimiter '|';
copy lineitem from '/home/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a' with delimiter '|';

