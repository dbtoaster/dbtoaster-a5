

#ifndef DATA_COLLECTION_FOR_ALGORITHMS
#define DATA_COLLECTION_FOR_ALGORITHMS

#include <boost/thread.hpp>

#include "ToasterConnection.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class DataCollection
        {
            
        public:
            
            DataCollection()
            {
                currentPrice=0;
                numberSamples=0;
                sampleVariance=0;
                populationVariance=0;
                M2=0;
                meanPrice=0;
                bids_diff=1;
                asks_diff=1;
                
                asksTime=1;
                bidsTime=1;
            }
            
            void readSOBIType()
            {
                boost::mutex::scoped_lock lock(mutex);
                
               get<0>(currentSOBI)=toaster.getBidsPV();
               get<1>(currentSOBI)=toaster.getBidsV();
               get<2>(currentSOBI)=0;
               get<3>(currentSOBI)=toaster.getAsksPV();
               get<4>(currentSOBI)=toaster.getAsksV();
               
               //inactive untill getting a 5 tuple
//               setCurrentPrice(get<2>(currentSOBI));

                setSOBIStats();
            }
            
            void readTimedSobiType()
            {
                boost::mutex::scoped_lock lock(mutex);
                
                get<0>(currentTimedSOBI)=0;
                get<1>(currentTimedSOBI)=toaster.getBidsPV();
                get<2>(currentTimedSOBI)=toaster.getBidsV();
                get<3>(currentTimedSOBI)=0;
                get<4>(currentTimedSOBI)=0;
                get<5>(currentTimedSOBI)=toaster.getAsksPV();
                get<6>(currentTimedSOBI)=toaster.getAsksV();  
                
                //setCurrentPrice(get<3>(currentTimedSOBI));
                         
            }
            
            void getMarketPlays()
            {
                boost::mutex::scoped_lock lock(mutex);
                
//                askPlays=toaster.getAskList();
//                bidPlays=toaster.getBidList();
            }
            
            void readBrokeType()
            {
                
            }
            
            void setCurrentPrice(double p)
            {
                currentPrice=p;
                updatePriceStats();
            }
            
            SOBItuple & getSOBITuple()
            {
                return currentSOBI;
            }
            
            timedSOBItuple & getTimedSOBITuple()
            {
                return currentTimedSOBI;
            }
            
            list<PVpair> & getAskPlays()
            {
                return askPlays;
            }
            
            list<PVpair> & getBidPlays()
            {
                return bidPlays;
            }
            
            int getCurrentPrice()
            {
                return currentPrice;
            }
            
            double getMeanPrice()
            {
                return meanPrice;
            }
            
            double getSampleVariance()
            {
                return sampleVariance;
            }
            
            double getBidsDiff()
            {
                return bids_diff;
            }
            
            double getAsksDiff()
            {
                return asks_diff;
            }
            
            long getNumberSamples()
            {
                return numberSamples;
            }
            
            double getAsksTime()
            {
                return asksTime;
            }
            
            double getBidsTime()
            {
                return bidsTime;
            }
            
        private:
            
            void setSOBIStats()
            {   
                
                if ( get<1>(currentSOBI) != 0 || get<4>(currentSOBI) != 0 ) {
                    bids_diff= currentPrice - get<0>(currentSOBI)/get<1>(currentSOBI);
                    asks_diff=get<3>(currentSOBI)/get<4>(currentSOBI) - currentPrice;
                }
                
            }
            
            void updatePriceStats()
            {
                boost::mutex::scoped_lock lock(mutex_price);
                
                numberSamples++;
                double delta = currentPrice - meanPrice;
                meanPrice = meanPrice + delta / numberSamples;
                M2 = M2 + delta * (currentPrice - meanPrice);
                
                sampleVariance = M2 / numberSamples;
                populationVariance = M2 / (numberSamples - 1);
            }
            
            void setSOBITimedStats()
            {
                asksTime=get<0>(currentTimedSOBI);
                bidsTime=get<4>(currentTimedSOBI);
            }
            
             boost::mutex mutex;
             boost::mutex mutex_price;
            
            ToasterConnection toaster;
            
            SOBItuple currentSOBI;
            timedSOBItuple currentTimedSOBI;
            
            double bids_diff;
            double asks_diff;
            
            double asksTime;
            double bidsTime;
            
            double currentPrice;
            double meanPrice;
            long   numberSamples;
            double sampleVariance;
            double populationVariance;
            double M2;
            
            list<PVpair> askPlays;
            list<PVpair> bidPlays;
            
            
        };
    }
}


#endif