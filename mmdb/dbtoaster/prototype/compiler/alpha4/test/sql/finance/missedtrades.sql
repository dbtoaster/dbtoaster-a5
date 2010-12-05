CREATE TABLE BIDS (t double, id int, broker_id int, p double, v double);
CREATE TABLE ASKS (t double, id int, broker_id int, p double, v double);

--select b.broker_id, sum(a.p*a.v+-1*(b.p*b.v))
--from bids b, asks a
--where 0.25*(select sum(a1.v) from asks a1) >
--           (select sum(a2.v) from asks a2 where a2.p > a.p)
--and   0.25*(select sum(b1.v) from bids b1) >
--           (select sum(b2.v) from bids b2 where b2.p > b.p)
--group by b.broker_id

-- simplified form for debugging
select b.broker_id, sum((a.p*a.v) + ((-1) * (b.p*b.v)))
from bids b, asks a
where 
--      0.25*(select sum(a1.v) from asks a1) >
--           (select sum(a2.v) from asks a2 where a2.p > a.p)
--and
      0.25*(select sum(b1.v) from bids b1) >
           (select sum(b2.v) from bids b2 where b2.p > b.p)
group by b.broker_id