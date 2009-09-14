

#ifndef ALGO_INTERFACE_FOR_DEMO
#define ALGO_INTERFACE_FOR_DEMO


#include "DataCollection.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

/*
 * Algorithms Interface serves to simplify implementation of 
 * different Algorithms and a way to run them.
 */
namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class AlgoInterface
        {
        public:
            //Common function to be implemented by all algorithms
            virtual void run(DataCollection & data, deque<AlgoMessages*> & messages) {};
            
        };
        
    };
};
#endif