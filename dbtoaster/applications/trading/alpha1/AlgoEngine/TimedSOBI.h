
#ifndef TIMED_SOBI_TRADING_ALGORITHM
#define TIMED_SOBI_TRADING_ALGORITHM


#include <iostream>

#include "DataTuple.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

/*
 * Similar to simple SOBI but adds also looks at time stamp imbalance
 */

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class TimedSOBI: public AlgoInterface
        {
        public:
            TimedSOBI()
            {
                numberSamples=0;
                meanPrice=0;
                minimumLimitSamples=400;
            }
            
            void run(DataCollection & data, deque<AlgoMessages*> & messages) 
            {
                double currentPrice=data.getCurrentPrice();
                
                double bids_diff = data.getBidsDiff();
                double asks_diff = data.getAsksDiff();
                
                double asksTime=data.getAsksTime();
                double bidsTime=data.getBidsTime();
                
                //there is a greater support for the buy side (bids are further from current price)
                //and the orders are older (smaller timestamp)
                
                if (asks_diff/bids_diff < 0.5 && bidsTime/asksTime < 0.8)
                {
                    DataTuple t; 
                    AlgoMessages * msg= new AlgoMessages();
                
                    t.t=0;
                    t.id=0;
                    t.b_id=13;
                    t.action="S";
                    t.price=currentPrice;
                    t.volume=100;
                
                    msg->tuple=t;
                    msg->type=1;
                
                    messages.push_back(msg);
                }
                
                //there is a greater support for the sell side (asks are further from current price)
                //and the orders are older (smaller timestamp)
            
                if (bids_diff/asks_diff < 0.5 && asksTime/bidsTime < 0.8 )
                {
                    DataTuple t; 
                    AlgoMessages * msg = new AlgoMessages();
                
                    t.t=0;
                    t.id=0;
                    t.b_id=13;
                    t.action="B";
                    t.price=currentPrice;
                    t.volume=100;
                
                    msg->tuple=t;
                    msg->type=1;
                
                    messages.push_back(msg);                    
                } 
                
            }
            
        private:
            
            long   numberSamples;
            int    minimumLimitSamples;
            double meanPrice;
            double M2;
            double sampleMean;
            double populationMean;
            
        };
        
    };
};


#endif