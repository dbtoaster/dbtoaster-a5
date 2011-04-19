
#ifndef SIMPLE_SOBI_TRADING_ALGORITHM
#define SIMPLE_SOBI_TRADING_ALGORITHM

#include <iostream>
#include <Math.h>

#include "DataTuple.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

/*
 * Basic SOBI Algorithm buys/sells when imbalance between 
 * books vwap and current price is 4 times larger for one side
 */

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
                
                if (currentPrice > 0)
                {
                    if (asks_diff/bids_diff < 0.25)
                    {
                        DataTuple t; 
                        AlgoMessages * msg= new AlgoMessages();
                    
                        t.t=0;
                        t.id=0;
                        t.b_id=10;
                        t.action="S";
                        t.price=currentPrice*10000;
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
                        t.price=currentPrice*10000;
                        t.volume=100;
                    
                        msg->tuple=t;
                        msg->type=1;
                    
                        messages.push_back(msg);                    
                    } 
                }
            }
            
        private:
            
        };
        
    };
};


#endif
