#ifndef DBTOASTER_ADAPTORS_H
#define DBTOASTER_ADAPTORS_H

#include <map>
#include "datasets.h"
#include "streamengine.h"

namespace DBToaster
{
    namespace Adaptors
    {
        using namespace DBToaster::StandaloneEngine;

        template <typename ResultType>
        struct Adaptor
        {
            typedef ResultType Result;
            virtual void operator()(DBToasterTuple&, boost::any&) = 0;
        };


        template<typename ResultType>
        struct InsertTupleAdaptor : public Adaptor<ResultType>
        {
            InsertTupleAdaptor(){}
            void operator()(DBToasterTuple& a, boost::any& b)
            {
                a.type = insertTuple;
                a.data = b;
            }
        };

        template<typename ResultType>
        struct DeleteTupleAdaptor : public Adaptor<ResultType>
        {
            DeleteTupleAdaptor() {}
            void operator()(DBToasterTuple& a, boost::any& b)
            {
                a.type = deleteTuple;
                a.data = b;
            }
        };
    };

    namespace DemoDatasets
    {
        using namespace std;
        using namespace DBToaster::StandaloneEngine;
        using namespace DBToaster::Adaptors;

        typedef struct {
            int t;
            int id;
            int broker_id;
            int price;
            int volume;
        } OrderbookHandlerInput;

        typedef map<int, tuple<double, int, double, double> > order_ids;
        //typedef map<int, tuple<double, double> > order_ids;

        order_ids bid_orders;
        order_ids ask_orders;

        struct OrderbookTupleAdaptor : public Adaptor<OrderbookHandlerInput>
        {

            OrderbookTupleAdaptor() {}

            void operator()(DBToasterTuple& a, boost::any& b)
            {
                OrderbookTuple* v = any_cast<OrderbookTuple>(&b);

                if ( v->action == "B" ) {
                    v->broker_id = ((int) rand()) % 9 + 1;
                    bid_orders[v->id] = make_tuple(v->t, v->broker_id, v->price, v->volume);
//                    bid_orders[v->id] = make_tuple(v->price, v->volume);

                    a.type = insertTuple;
                }

                else if ( v->action == "S" ) {
  		    v->broker_id = ((int) rand()) % 9 + 1;
                    //ask_orders[v->id] = make_tuple(v->price, v->volume);
                    ask_orders[v->id] = make_tuple(v->t, v->broker_id, v->price, v->volume);
                    a.type = insertTuple;
                }

                else if ( v->action == "E" )
                    // This is an update, how do generate an additional insert?
                    a.type = deleteTuple;

                else if ( v->action == "F" )
                {
                    order_ids::iterator bid_found = bid_orders.find(v->id);
                    if ( bid_found != bid_orders.end() )
                    {
			v->t = get<0>(bid_found->second);
			v->broker_id = get<1>(bid_found->second);
                        v->price = get<2>(bid_found->second);
                        v->volume = get<3>(bid_found->second);
                        bid_orders.erase(bid_found);
                    }
                    else
                    {
                        order_ids::iterator ask_found = ask_orders.find(v->id);
                        if ( ask_found != ask_orders.end() )
                        {
			    v->t = get<0>(ask_found->second);
			    v->broker_id = get<1>(ask_found->second);
                            v->price = get<2>(ask_found->second);
                            v->volume = get<3>(ask_found->second);
                            ask_orders.erase(ask_found);
                        }
                        // TODO: handle invalid tuples that are neither bids nor sell...
                    }

                    a.type = deleteTuple;
                }

                else if ( v->action == "D" )
                {
                    order_ids::iterator bid_found = bid_orders.find((int)(v->id));
                    if ( bid_found != bid_orders.end() )
                    {
			v->t = get<0>(bid_found->second);
			v->broker_id = get<1>(bid_found->second);
                        v->price = get<2>(bid_found->second);
                        v->volume = get<3>(bid_found->second);
                        bid_orders.erase(bid_found);
                    }
                    else
                    {
                        order_ids::iterator ask_found = ask_orders.find(v->id);
                        if ( ask_found != ask_orders.end() )
                        {
			    v->t = get<0>(ask_found->second);
			    v->broker_id = get<1>(ask_found->second);
                            v->price = get<2>(ask_found->second);
                            v->volume = get<3>(ask_found->second);
                            ask_orders.erase(ask_found);
                        }
                        // TODO: handle invalid tuples that are neither bids nor sell...
                    }

                    a.type = deleteTuple;
                }

                /*
                // ignore for now...
                else if ( v->action == "X")
                else if ( v->action == "C")
                else if ( v->action == "T")
                */
 
                // TODO: casting from doubles to ints
                OrderbookHandlerInput r;

                r.t = v->t;
                r.id = v->id;
                r.broker_id = v->broker_id;
                r.price = v->price;
                r.volume = v->volume;
                a.data = r;

//		cout << v->action << " " << v->t << " " << v->id << " " << v->broker_id << " " << v->price << " " << v->volume <<endl;
            }
        };

        typedef InsertTupleAdaptor<lineitem> LineitemTupleAdaptor;
        typedef InsertTupleAdaptor<order> OrderTupleAdaptor;
        typedef InsertTupleAdaptor<part> PartTupleAdaptor;
        typedef InsertTupleAdaptor<customer> CustomerTupleAdaptor;
        typedef InsertTupleAdaptor<supplier> SupplierTupleAdaptor;
        typedef InsertTupleAdaptor<partsupp> PartSuppTupleAdaptor;
        typedef InsertTupleAdaptor<nation> NationTupleAdaptor;
        typedef InsertTupleAdaptor<region> RegionTupleAdaptor;
        typedef InsertTupleAdaptor<LinearRoadTuple> LinearRoadTupleAdaptor;
    };
};
    
#endif
