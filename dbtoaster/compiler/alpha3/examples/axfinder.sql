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

select b.broker_id, sum(a.v+-1*b.v)
from bids b, asks a
where b.broker_id = a.broker_id
and ( (a.p+-1*b.p > 1000) or (b.p+-1*a.p > 1000) )
group by b.broker_id