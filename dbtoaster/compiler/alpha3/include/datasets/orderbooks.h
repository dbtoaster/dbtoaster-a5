#ifndef ORDERBOOK_H
#define ORDERBOOK_H

#include <string>

namespace DBToaster
{
    namespace DemoDatasets
    {
        using namespace std;

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
