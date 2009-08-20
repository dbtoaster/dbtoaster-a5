
#ifndef SIMPLE_SOBI_TRADING_ALGORITHM
#define SIMPLE_SOBI_TRADING_ALGORITHM

#include <iostream>

#include "DataTuple.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class SimpleSOBI: public AlgoInterface
        {
        public:
            SimpleSOBI()
            {
                
            }
            
            void run(DataCollection & data, deque<AlgoMessages*> & messages) 
            {
                double currentPrice=data.getCurrentPrice();
                
                double bids_diff = data.getBidsDiff();
                double asks_diff = data.getAsksDiff();
                
                
                if (asks_diff/bids_diff < 0.25)
                {
                    DataTuple t; 
                    AlgoMessages * msg= new AlgoMessages();
                    
                    t.t=0;
                    t.id=0;
                    t.b_id=10;
                    t.action="S";
                    t.price=currentPrice;
                    t.volume=100;
                    
                    msg->tuple=t;
                    msg->type=1;
                    
                    messages.push_back(msg);
                }
                
                if (bids_diff/asks_diff < 0.25)
                {
                    DataTuple t; 
                    AlgoMessages * msg = new AlgoMessages();
                    
                    t.t=0;
                    t.id=0;
                    t.b_id=10;
                    t.action="B";
                    t.price=currentPrice;
                    t.volume=100;
                    
                    msg->tuple=t;
                    msg->type=1;
                    
                    messages.push_back(msg);                    
                } 
            }
            
        private:
            
        };
        
    };
};


#endif