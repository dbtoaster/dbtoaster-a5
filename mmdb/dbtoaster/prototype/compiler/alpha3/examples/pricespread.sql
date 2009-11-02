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


-- look at spread between significant orders
select sum(a.p+-1*b.p) from bids b, asks a
where ( b.v>0.0001*(select sum(b1.p) from bids b1) )
and ( a.v<0.0001*(select sum(a1.p) from asks a1) )