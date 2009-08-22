

#ifndef ALGORITHM_ENGINE_ALGORITHM_DEMO
#define ALGORITHM_ENGINE_ALGORITHM_DEMO

#include <list>
#include <unistd.h>

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/math/common_factor.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/any.hpp>


#include "DataTuple.h"
#include "WriterConnection.h"
#include "OrderManager.h"
#include "DataCollection.h"
#include "AlgoInterface.h"
#include "AlgoTypeDefs.h"


using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class AlgorithmsEngine
        {
        public:

            AlgorithmsEngine(boost::asio::io_service& io_service, boost::asio::io_service& io_service_reader, int shares, int m):
            localIOService(io_service),
               manager(io_service, shares, m)
            {
                manager.startReading(io_service_reader);
 //               boost::thread t(boost::bind(&boost::asio::io_service::run, &io_service_reader));
            }

            void runAlgos()
            {
//                cout<<"runAlgos: got in"<<endl;
                getData();
                
//                cout<<"runAlgos: got data"<<endl;
                cleanMessages();
                
//                cout<<"runAlgos: cleaned messages"<<endl;
                
                list<AlgoInterface*>::iterator algo_ptr=my_algos.begin();

                for(; algo_ptr != my_algos.end(); algo_ptr++)
                {
                    (*algo_ptr)->run(data, messages);
 //                   datum++;
                }
//                cout<<"runAlgos: ran reach algo "<<endl;

                manager.sendOrders(boost::bind(&DBToaster::DemoAlgEngine::AlgorithmsEngine::runAlgos, this), messages);
//                usleep(200);
//                cout<<"runAlgos: done send messages"<<endl;

            }

            void addAlgo(AlgoInterface * algo)
            {
                my_algos.push_back(algo);
            }
            
            OrderManager * getOrderManager()
            {
                return &manager;
            }
            
            DataCollection * getDataCollection()
            {
                return &data;
            }

        private:

            void getData()
            {
                data.readSOBIType();
                data.readTimedSobiType();
                data.getMarketPlays();
                data.setCurrentPrice(manager.getCurrentStockPrice());
//                data.readBrokeType();
            }
            
            void cleanMessages()
            {
                while (!messages.empty())
                {
                    AlgoMessages * msg=messages.front();
                    messages.pop_front();
                    delete msg;
                }
            }

            list<AlgoInterface*> my_algos;
            DataCollection       data;
            deque<AlgoMessages*> messages;

            boost::asio::io_service io_service_reader;
            boost::asio::io_service& localIOService;
            

            OrderManager manager;
        };

    };
};

#endif