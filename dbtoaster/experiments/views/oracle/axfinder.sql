DROP MATERIALIZED VIEW LOG ON BIDS;
DROP MATERIALIZED VIEW LOG ON ASKS;

DROP MATERIALIZED VIEW AX;

DROP TABLE BIDS;
DROP TABLE ASKS;

CREATE TABLE BIDS (t double precision, id int, broker_id int, p double precision, v double precision);
CREATE TABLE ASKS (t double precision, id int, broker_id int, p double precision, v double precision);

CREATE MATERIALIZED VIEW LOG ON BIDS
    WITH ROWID, SEQUENCE(broker_id,p,v)
    INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON ASKS
    WITH ROWID, SEQUENCE(broker_id,p,v)
    INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW AX
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS
select b.broker_id, sum(a.v+-1*b.v) as axvol
from bids b, asks a
where b.broker_id = a.broker_id
and ( (a.p > b.p+1000) or (b.p > a.p+1000) )
group by b.broker_id;

--CREATE MATERIALIZED VIEW AX
--BUILD IMMEDIATE
--REFRESH COMPLETE ON NEXT SYSTIMESTAMP+TO_DSINTERVAL('0 0:0:0.1')
--AS
--select b.broker_id, sum(a.v+-1*b.v)
--from bids b, asks a
--where b.broker_id = a.broker_id
--and ( (a.p+-1*b.p > 1000) or (b.p+-1*a.p > 1000) )
--group by b.broker_id;
