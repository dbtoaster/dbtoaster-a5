CREATE TABLE BIDS (t double, id int, broker_id int, p double, v double)
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderbookFileStream'
    ARGS '"/Users/yanif/tmp/data/cleanedData.csv",10000'
    INSTANCE 'VwapBids'
    TUPLE 'DBToaster::DemoDatasets::OrderbookTuple'
    ADAPTOR 'DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor'
    BINDINGS 't,t,id,id,broker_id,broker_id,p,price,v,volume';

-- two level nesting: VWAP
SELECT sum(B.P*B.V) FROM BIDS B
    WHERE 0.25*(SELECT sum(B1.V) FROM BIDS B1) >
        (SELECT sum(B2.V) FROM BIDS B2 WHERE B2.P > B.P);

/*
-- three level nesting
-- Note: this currently doesn't compile correctly
SELECT sum(B2.P*B2.V) FROM BIDS B2
    WHERE 0.25*(SELECT sum(BIDS.V) FROM BIDS) >
        (SELECT sum(B1.V) FROM BIDS B1
         WHERE
             B1.V < (SELECT sum(B3.V) FROM BIDS B3 WHERE B1.P < B3.P)
             AND B1.P > B2.P);
*/