DROP MATERIALIZED VIEW LOG ON BIDS;
DROP MATERIALIZED VIEW LOG ON ASKS;

DROP MATERIALIED VIEW PS;

DROP TABLE BIDS;
DROP TABLE ASKS;

CREATE TABLE BIDS (t double precision, id int, broker_id int, p double precision, v double precision);
CREATE TABLE ASKS (t double precision, id int, broker_id int, p double precision, v double precision);

CREATE MATERIALIZED VIEW LOG ON BIDS
    WITH ROWID, SEQUENCE(p,v)
    INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON ASKS
    WITH ROWID, SEQUENCE(p,v)
    INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW PS
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS
-- look at spread between significant orders
select sum(a.p+-1*b.p) from bids b, asks a
where ( b.v>0.0001*(select sum(b1.v) from bids b1) )
and ( a.v>0.0001*(select sum(a1.v) from asks a1) );


--CREATE MATERIALIZED VIEW PS
--BUILD IMMEDIATE
--REFRESH COMPLETE NEXT SYSTIMESTAMP + TO_DSINTERVAL('0 0:0:0.1')
--AS
-- look at spread between significant orders
--select sum(a.p+-1*b.p) from bids b, asks a
--where ( b.v>0.0001*(select sum(b1.v) from bids b1) )
--and ( a.v>0.0001*(select sum(a1.v) from asks a1) );