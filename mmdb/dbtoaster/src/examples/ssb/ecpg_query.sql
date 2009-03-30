/*
-- Materialize SSB tables

drop table if exists ssb_date;
drop table if exists ssb_customer;
drop table if exists ssb_supplier;
drop table if exists ssb_part;
drop table if exists ssb_lineorder;
drop table if exists ssb_totalprices;

select
    orderdate as datekey,
    extract(year from orderdate) as year
into ssb_date
from orders;

create index dk_h_idx on ssb_date using hash (datekey);
create index dk_b_idx on ssb_date using btree (datekey);
create index dy_h_idx on ssb_date using hash (year);


select custkey, customer.name, address,
    (substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
    nation.name as nation,
    region.name as region,
    phone, mktsegment
into ssb_customer
from customer, nation, region
where customer.nationkey = nation.nationkey
and nation.regionkey = region.regionkey;

create index ck_h_idx on ssb_customer using hash (custkey);
create index cr_h_idx on ssb_customer using hash (region);
create index cn_h_idx on ssb_customer using hash (nation);
create index cr_b_idx on ssb_customer using btree (region);
create index ckr_b_idx on ssb_customer using btree (custkey, region);


select suppkey, supplier.name, address,
    (substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
    nation.name as nation,
    region.name as region,
    phone
into ssb_supplier
from supplier, nation, region
where supplier.nationkey = nation.nationkey
and nation.regionkey = region.regionkey;

create index sk_h_idx on ssb_supplier using hash (suppkey);
create index sr_h_idx on ssb_supplier using hash (region);
create index sr_b_idx on ssb_supplier using btree (region);
create index skr_b_idx on ssb_supplier using btree (suppkey, region);


select partkey, name, mfgr,
    brand as category,
    (brand || (cast (round(random()*25) as text))) as brand1,
    substring(name from 0 for position(' ' in name)) as color,
    type, size, container
into ssb_part
from part;

create index pk_h_idx on ssb_part using hash (partkey);
create index pm_h_idx on ssb_part using hash (mfgr);
create index pm_b_idx on ssb_part using btree (mfgr);
create index pkm_b_idx on ssb_part using btree (partkey, mfgr);


select orderkey,
    sum((extendedprice*(100-discount)*(100+tax))/100) as totalprice
into ssb_totalprices
from lineitem group by orderkey;

select
    lineitem.orderkey, linenumber,
    custkey, partkey, suppkey,
    orderdate, orderpriority,
    shippriority, quantity, extendedprice,
    ssb_totalprices.totalprice as ordtotalprice, discount,
    (extendedprice * (100-discount)/100) as revenue,
    (90000 + ((partkey/10) % 20001) + ((partkey % 1000)*100)) as supplycost,
    tax, commitdate, shipmode
into ssb_lineorder
from lineitem, orders, ssb_totalprices
where lineitem.orderkey = orders.orderkey
and orders.orderkey = ssb_totalprices.orderkey;

create index lcspo_b_idx on ssb_lineorder using btree (custkey, suppkey, partkey, orderdate);

*/


-- Views

drop view if exists ssb_date;
drop view if exists ssb_customer;
drop view if exists ssb_supplier;
drop view if exists ssb_part;
drop view if exists ssb_lineorder;
drop view if exists ssb_totalprices;

create view ssb_date as
select
    orderdate as datekey,
    extract(year from orderdate) as year
from orders;

create index dk_h_idx on ssb_date using hash (datekey);
create index dk_b_idx on ssb_date using btree (datekey);
create index dy_h_idx on ssb_date using hash (year);


create view ssb_customer as
select custkey, customer.name, address,
    (substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
    nation.name as nation,
    region.name as region,
    phone, mktsegment
from customer, nation, region
where customer.nationkey = nation.nationkey
and nation.regionkey = region.regionkey;

--create index ck_h_idx on ssb_customer using hash (custkey);
--create index cr_h_idx on ssb_customer using hash (region);
--create index cn_h_idx on ssb_customer using hash (nation);
--create index cr_b_idx on ssb_customer using btree (region);
--create index ckr_b_idx on ssb_customer using btree (custkey, region);


create view ssb_supplier as
select suppkey, supplier.name, address,
    (substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
    nation.name as nation,
    region.name as region,
    phone
from supplier, nation, region
where supplier.nationkey = nation.nationkey
and nation.regionkey = region.regionkey;

--create index sk_h_idx on ssb_supplier using hash (suppkey);
--create index sr_h_idx on ssb_supplier using hash (region);
--create index sr_b_idx on ssb_supplier using btree (region);
--create index skr_b_idx on ssb_supplier using btree (suppkey, region);


create view ssb_part as
select partkey, name, mfgr,
    brand as category,
    (brand || (cast (round(random()*25) as text))) as brand1,
    substring(name from 0 for position(' ' in name)) as color,
    type, size, container
from part;

--create index pk_h_idx on ssb_part using hash (partkey);
--create index pm_h_idx on ssb_part using hash (mfgr);
--create index pm_b_idx on ssb_part using btree (mfgr);
--create index pkm_b_idx on ssb_part using btree (partkey, mfgr);


create view ssb_totalprices as
select orderkey,
    sum((extendedprice*(100-discount)*(100+tax))/100) as totalprice
from lineitem group by orderkey;

create view ssb_lineorder as
select
    lineitem.orderkey, linenumber,
    custkey, partkey, suppkey,
    orderdate, orderpriority,
    shippriority, quantity, extendedprice,
    ssb_totalprices.totalprice as ordtotalprice, discount,
    (extendedprice * (100-discount)/100) as revenue,
    (90000 + ((partkey/10) % 20001) + ((partkey % 1000)*100)) as supplycost,
    tax, commitdate, shipmode
from lineitem, orders, ssb_totalprices
where lineitem.orderkey = orders.orderkey
and orders.orderkey = ssb_totalprices.orderkey;

--create index lcspo_b_idx on ssb_lineorder using btree (custkey, suppkey, partkey, orderdate);



select d.year, c.nation,
    sum(lo.revenue - lo.supplycost) as profit
from
    ssb_date as d,
    ssb_customer as c,
    ssb_supplier as s,
    ssb_part as p,
    ssb_lineorder as lo
where lo.custkey = c.custkey
and lo.suppkey = s.suppkey
and lo.partkey = p.partkey
and lo.orderdate = d.datekey
and c.region = 'AMERICA'
and s.region = 'AMERICA'
and (p.mfgr = 'Manufacturer#1' or p.mfgr = 'Manufacturer#2')
group by d.year, c.nation;


/*	
select d.year, c.nation,
	sum(lo.revenue - lo.supplycost) as profit
from
	(select
			orderdate as datekey,
			extract(year from orderdate) as year
		from orders)
	as d,
	(select custkey, customer.name, address,
			(substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
			nation.name as nation,
			region.name as region,
			phone, mktsegment
		from customer, nation, region
		where customer.nationkey = nation.nationkey
		and nation.regionkey = region.regionkey)
	as c,
	(select suppkey, supplier.name, address,
			(substr(nation.name, 0, 9) || (cast (round(random() * 9) as text))) as city,
			nation.name as nation,
			region.name as region,
			phone
		from supplier, nation, region
		where supplier.nationkey = nation.nationkey
		and nation.regionkey = region.regionkey)
	as s,
	(select partkey, name, mfgr,
			brand as category,
			(brand || (cast (round(random()*25) as text))) as brand1,
			substring(name from 0 for position(' ' in name)) as color,
			type, size, container
		from part)
	as p,
	(select
			lineitem.orderkey, linenumber,
			custkey, partkey, suppkey,
			orderdate, orderpriority,
			shippriority, quantity, extendedprice,
			TP.totalprice as ordtotalprice, discount,
			(extendedprice * (100-discount)/100) as revenue,
			(90000 + ((partkey/10) % 20001) + ((partkey % 1000)*100)) as supplycost,
			tax, commitdate, shipmode
		from lineitem, orders,
			(select orderkey,
				sum((extendedprice*(100-discount)*(100+tax))/100) as totalprice
				from lineitem group by orderkey) as TP
		where lineitem.orderkey = orders.orderkey
		and orders.orderkey = TP.orderkey)
	as lo
where lo.custkey = c.custkey
and lo.suppkey = s.suppkey
and lo.partkey = p.partkey
and lo.orderdate = d.datekey
and c.region = 'AMERICA'
and s.region = 'AMERICA'
and (p.mfgr = 'Manufacturer#1' or p.mfgr = 'Manufacturer#2')
group by d.year, c.nation
order by d.year, c.nation;
*/
