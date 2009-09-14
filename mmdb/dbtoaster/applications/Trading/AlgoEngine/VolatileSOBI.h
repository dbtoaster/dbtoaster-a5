
#ifndef VOLATILE_SOBI_TRADING_ALGORITHM
#define VOLATILE_SOBI_TRADING_ALGORITHM


#include <iostream>

#include "DataTuple.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;


/*
 * This is a variation of Simple SOBI strategy 
 * the decision to buy/sell depends on standart deviation of the current price
 */

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class VolatileSOBI: public AlgoInterface
        {
        public:
            VolatileSOBI(int n)
            {
                minimumLimitSamples=n;
            }
            
            void run(DataCollection & data, deque<AlgoMessages*> & messages) 
            {
                
                double currentPrice=data.getCurrentPrice();
                double sampleVariance =data.getSampleVariance();
                
                double bids_diff = data.getBidsDiff();
                double asks_diff = data.getAsksDiff();
                
                long numberSamples = data.getNumberSamples();
                
                if (numberSamples > minimumLimitSamples) 
                {
                    if (currentPrice > 0)
                    {
                        //check for SD of the current price
                        if (asks_diff/bids_diff < sampleVariance/bids_diff )
                        {
                            DataTuple t; 
                            AlgoMessages * msg= new AlgoMessages();
                    
                            t.t=0;
                            t.id=0;
                            t.b_id=11;
                            t.action="S";
                            t.price=currentPrice*10000;
                            t.volume=100;
                    
                            msg->tuple=t;
                            msg->type=1;
                    
                            messages.push_back(msg);
                        }
                        
                        //reverce 
                        if (bids_diff/asks_diff < sampleVariance /asks_diff )
                        {
                            DataTuple t; 
                            AlgoMessages * msg = new AlgoMessages();
                    
                            t.t=0;
                            t.id=0;
                            t.b_id=11;
                            t.action="B";
                            t.price=currentPrice*10000;
                            t.volume=100;
                    
                            msg->tuple=t;
                            msg->type=1;
                    
                            messages.push_back(msg);                    
                        } 
                    }
                }
            }
            
        private:
            
            int minimumLimitSamples;
        
        };
    };
};


#endif