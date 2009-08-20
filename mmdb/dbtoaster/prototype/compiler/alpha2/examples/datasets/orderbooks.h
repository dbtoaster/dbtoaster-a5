#ifndef ORDERBOOK_H
#define ORDERBOOK_H

namespace DBToaster
{
    namespace DemoDatasets
    {
        // Timestamp, order id, action, volume, price
        struct OrderbookTuple {
            double t;
            int id;
            int broker_id;
            string action;
            double volume;
            double price;
        };
    };
};

#endif
