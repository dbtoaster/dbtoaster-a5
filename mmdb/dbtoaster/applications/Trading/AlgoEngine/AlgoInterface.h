

#ifndef ALGO_INTERFACE_FOR_DEMO
#define ALGO_INTERFACE_FOR_DEMO


#include "DataCollection.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class AlgoInterface
        {
        public:
            virtual void run(DataCollection & data, deque<AlgoMessages*> & messages) {};
            
        };
        
    };
};
#endif