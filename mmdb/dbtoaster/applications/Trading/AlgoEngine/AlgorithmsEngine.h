

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

            //constructor initating io_services puts number manager and so on.
            AlgorithmsEngine(boost::asio::io_service& io_service, boost::asio::io_service& io_service_reader, int shares, int m):
            localIOService(io_service),
               manager(io_service, shares, m)
            {
                //initates reading client
                manager.startReading(io_service_reader);
            }

            //main execution loop 
            void runAlgos()
            {
                //makes updates to queries needed by the algorithms
                getData();
                //simple clean up
                cleanMessages();
                
                list<AlgoInterface*>::iterator algo_ptr=my_algos.begin();

                //check if any of the algorithms want to trade stocks
                for(; algo_ptr != my_algos.end(); algo_ptr++)
                {
                    (*algo_ptr)->run(data, messages);
                }
                
                //execute stock trades if needed
                manager.sendOrders(boost::bind(&DBToaster::DemoAlgEngine::AlgorithmsEngine::runAlgos, this), messages);
            }

            //adds a new algorithm to the mix
            void addAlgo(AlgoInterface * algo)
            {
                my_algos.push_back(algo);
            }
            
            //returns a pointer to otder manageer
            OrderManager * getOrderManager()
            {
                return &manager;
            }
            
            //gets Data Collection
            DataCollection * getDataCollection()
            {
                return &data;
            }

        private:

            //gets results for each query and updates needed values for each algorithm
            //improvement make an interface query running and do it in a loop
            //this will allow to add queries on a fly
            void getData()
            {
                data.readSOBIType();
                data.readTimedSobiType();
                data.getMarketPlays();
                data.setCurrentPrice(manager.getCurrentStockPrice());
//                data.readBrokeType();
            }
            
            //remove already sent messages
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
            DataCollection       data;        //interface to DBToaster Runtime
            OrderManager         manager;     //interface to Exchange Simulator
            deque<AlgoMessages*> messages;

            //io_services to run two independent execution paths
            boost::asio::io_service io_service_reader;
            boost::asio::io_service& localIOService;
                  
        };

    };
};

#endif