#ifndef DBTOASTER_ADAPTORS_H
#define DBTOASTER_ADAPTORS_H

#include <map>
#include <tr1/cstdint>

#include "datasets.h"
#include "standalone/streamengine.h"

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
        using namespace tr1;
        using namespace DBToaster::StandaloneEngine;
        using namespace DBToaster::Adaptors;

        typedef struct {
            double t;
            int    id;
            int    broker_id;
            double price;
            double volume;
        } OrderbookHandlerInput;

        enum OrderbookType { Bids, Asks };

        typedef map<int, tuple<double, int, double, double> > order_ids;
        //typedef map<int, tuple<double, double> > order_ids;

        struct OrderbookTupleAdaptor : public Adaptor<OrderbookHandlerInput>
        {
            OrderbookType book_type;
            order_ids bid_orders;
            order_ids ask_orders;

            OrderbookTupleAdaptor(OrderbookType t) : book_type(t){}

            void operator()(DBToasterTuple& a, boost::any& b)
            {
                OrderbookTuple* v = any_cast<OrderbookTuple>(&b);

                if ( v->action == "B" && book_type == Bids)
                {
                    v->broker_id = ((int) rand()) % 9 + 1;
                    //bid_orders[v->id] = make_tuple(v->price, v->volume);
                    bid_orders[v->id] = make_tuple(v->t, v->broker_id, v->price, v->volume);
                    a.type = insertTuple;
                }

                else if ( v->action == "S" && book_type == Asks )
                {
  		    v->broker_id = ((int) rand()) % 9 + 1;
                    //ask_orders[v->id] = make_tuple(v->price, v->volume);
                    ask_orders[v->id] = make_tuple(v->t, v->broker_id, v->price, v->volume);
                    a.type = insertTuple;
                }

                else if ( v->action == "E" ) {
                    // This is an update, how do generate an additional insert?
                    //a.type = deleteTuple;

                    // drop for now...
                    a.type = dropTuple;
                }

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
                        a.type = deleteTuple;
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
                            a.type = deleteTuple;
                        }
                        // drop invalid tuples that are neither bids nor sell...
                        else {
                            a.type = dropTuple;
                        }
                    }
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
                        a.type = deleteTuple;
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
                            a.type = deleteTuple;
                        }
                        // drop invalid tuples that are neither bids nor sell...
                        else {
                            a.type = dropTuple;
                        }
                    }
                }

                /*
                // ignore these types for now, dropping tuples...
                else if ( v->action == "X" ||  v->action == "C" || v->action == "T" )
                {
                    a.type = dropTuple;
                    return;
                }
                */

                // drop any tuples that have not been processed yet.
                else {
                    a.type = dropTuple;
                    return;
                }
 
                // TODO: casting from doubles to ints
                OrderbookHandlerInput r;

                r.t = v->t;
                r.id = v->id;
                r.broker_id = v->broker_id;
                r.price = v->price;
                r.volume = v->volume;
                a.data = r;

                // cout << v->action << " " << v->t << " " << v->id << " "
                //     << v->broker_id << " " << v->price << " " << v->volume <<endl;
            }
        };

        // Explicitly types for bid and asks streams since we currently
        // cannot pass in arguments to the adaptor from the SQL parser.
        struct BidsOrderbookTupleAdaptor : public OrderbookTupleAdaptor
        {
            BidsOrderbookTupleAdaptor() : OrderbookTupleAdaptor(Bids) {}
        };

        struct AsksOrderbookTupleAdaptor : public OrderbookTupleAdaptor
        {
            AsksOrderbookTupleAdaptor() : OrderbookTupleAdaptor(Asks) {}
        };

        // TPC-H adaptors, which are insert only for now...
        typedef InsertTupleAdaptor<lineitem> LineitemTupleAdaptor;
        typedef InsertTupleAdaptor<order> OrderTupleAdaptor;
        typedef InsertTupleAdaptor<part> PartTupleAdaptor;
        typedef InsertTupleAdaptor<customer> CustomerTupleAdaptor;
        typedef InsertTupleAdaptor<supplier> SupplierTupleAdaptor;
        typedef InsertTupleAdaptor<partsupp> PartSuppTupleAdaptor;
        typedef InsertTupleAdaptor<nation> NationTupleAdaptor;
        typedef InsertTupleAdaptor<region> RegionTupleAdaptor;

        // Simpified TPC-H types
        struct SimpleLineitem
        {
            int64_t orderkey;
            int64_t partkey;
            int64_t suppkey;
            int     linenumber;
            double  quantity;
            double  extendedprice;
            double  discount;
            double  tax;

            SimpleLineitem() {}
            SimpleLineitem(lineitem& a)
            {
                orderkey = a.orderkey;
                partkey = a.partkey;
                suppkey = a.suppkey;
                linenumber = a.linenumber;
                quantity = a.quantity;
                extendedprice = a.extendedprice;
                discount = a.discount;
                tax = a.tax;
            }
        };

        struct LineitemSimpleTupleAdaptor : public InsertTupleAdaptor<SimpleLineitem>
        {
            void operator()(DBToasterTuple& a, boost::any& b)
            {
                lineitem* v = boost::any_cast<lineitem>(&b);
                if ( v == NULL )
                    cout << "Invalid lineitem, found type " << b.type().name() << endl;

                assert( v != NULL );

                SimpleLineitem r(*v);
                a.data = r;
            }
        };


        typedef InsertTupleAdaptor<LinearRoadTuple> LinearRoadTupleAdaptor;
    };
};
    
#endif
