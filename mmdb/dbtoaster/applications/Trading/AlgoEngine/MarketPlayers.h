
#ifndef MARKET_PLAYERS_TRADING_ALGORITHM
#define MARKET_PLAYERS_TRADING_ALGORITHM


#include <iostream>

#include "DataTuple.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

/*
 * Algorithm for Market players. 
 * it detects when a big order on one side of the order book 
 * is used to create a match for a smaller order on the other side of the order book.
 *
 * Needs support for from Compiler
 */ 

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class MarketPlayers: public AlgoInterface
        {
        public:
            MarketPlayers(double pShares, double pPrice)
            {
                percentShares=pShares;
                priceShift=pPrice;
            }
            
            void run(DataCollection & data, deque<AlgoMessages*> & messages) 
            {
//                timedSOBItuple currentState = data.getTimedSOBITuple();
                double currentPrice=data.getCurrentPrice();
                list<PVpair> askPlays=data.getAskPlays();
                
                while (!askPlays.empty())
                {
                    PVpair pair=askPlays.front();
                    
                    DataTuple t; 
                    AlgoMessages * msg= new AlgoMessages();
                
                    t.t=0;
                    t.id=0;
                    t.b_id=14;
                    t.action="S";
                    t.price=(currentPrice+get<0>(pair))*priceShift;
                    t.volume=get<1>(pair)*percentShares;;
                
                    msg->tuple=t;
                    msg->type=1;
                    
                    askPlays.pop_front();
                
                    messages.push_back(msg);
                }
                
                list<PVpair> bidPlays=data.getBidPlays();
                
                while (!bidPlays.empty())
                {
                    PVpair pair=bidPlays.front();
                    
                    DataTuple t; 
                    AlgoMessages * msg= new AlgoMessages();
                
                    t.t=0;
                    t.id=0;
                    t.b_id=14;
                    t.action="B";
                    t.price=(currentPrice+get<0>(pair))*priceShift;
                    t.volume=get<1>(pair)*percentShares;;
                
                    msg->tuple=t;
                    msg->type=1;
                    
                    bidPlays.pop_front();
                
                    messages.push_back(msg);
                }
                
                
                
            }
            
        private:
            
            double percentShares;
            double priceShift;
            
        };
        
    };
};


#endif