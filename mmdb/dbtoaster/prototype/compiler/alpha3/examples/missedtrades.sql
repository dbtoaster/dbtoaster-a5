CREATE TABLE BIDS (t double, id int, broker_id int, p double, v double)
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderbookFileStream'
    ARGS '"/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000'
    INSTANCE 'VwapBids'
    TUPLE 'DBToaster::DemoDatasets::OrderbookTuple'
    ADAPTOR 'DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor'
    BINDINGS 't,t,id,id,broker_id,broker_id,p,price,v,volume';

CREATE TABLE ASKS (t double, id int, broker_id int, p double, v double)
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderbookFileStream'
    ARGS '"/home/yanif/datasets/orderbook/MSFT/20081201.csv",10000'
    INSTANCE 'VwapAsks'
    TUPLE 'DBToaster::DemoDatasets::OrderbookTuple'
    ADAPTOR 'DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor'
    BINDINGS 't,t,id,id,broker_id,broker_id,p,price,v,volume';

select b.broker_id, sum(a.p*a.v+-1*(b.p*b.v))
from bids b, asks a
where 0.25*(select sum(a1.v) from asks a1) >
           (select sum(a2.v) from asks a2 where a2.p > a.p)
and   0.25*(select sum(b1.v) from bids b1) >
           (select sum(b2.v) from bids b2 where b2.p > b.p)
group by b.broker_id