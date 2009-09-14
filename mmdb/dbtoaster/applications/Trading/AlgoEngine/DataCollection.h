

#ifndef DATA_COLLECTION_FOR_ALGORITHMS
#define DATA_COLLECTION_FOR_ALGORITHMS

#include <boost/thread.hpp>

#include "ToasterConnection.h"
#include "AlgoTypeDefs.h"

using namespace std;
using namespace boost;
using namespace DBToaster::DemoAlgEngine;


/*
* Interface to the DBToaster runtime. 
* Stores all the information needed by the algorithms.
* 
* This class can be remade to be more modular so that queries 
* and result processing functions be loaded at any point.
*/

namespace DBToaster
{
    namespace DemoAlgEngine
    {
        class DataCollection
        {

        public:

            //Constructor. Initiates all variables
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

            //reads SOBI Results from Runtime using DBToaster
            void readSOBIType()
            {
                boost::mutex::scoped_lock lock(mutex);
                double PVbids=toaster.getBidsPV();
                double PVasks=toaster.getAsksPV();

                get<0>(currentSOBI)=PVbids;
                get<1>(currentSOBI)=toaster.getBidsV();
                get<2>(currentSOBI)=currentPrice;
                get<3>(currentSOBI)=toaster.getAsksPV();
                get<4>(currentSOBI)=toaster.getAsksV();

               //makes statistics needed by the algorithm
                setSOBIStats();
            }

            //Timed SOBI currently not really working
            void readTimedSobiType()
            {
                boost::mutex::scoped_lock lock(mutex);

                get<0>(currentTimedSOBI)=0;
                get<1>(currentTimedSOBI)=toaster.getBidsPV();
                get<2>(currentTimedSOBI)=toaster.getBidsV();
                get<3>(currentTimedSOBI)=currentPrice;
                get<4>(currentTimedSOBI)=0;
                get<5>(currentTimedSOBI)=toaster.getAsksPV();
                get<6>(currentTimedSOBI)=toaster.getAsksV();  


            }

            //Implements market player policy not implemented yet
            //no query support
            void getMarketPlays()
            {
                boost::mutex::scoped_lock lock(mutex);

//                askPlays=toaster.getAskList();
//                bidPlays=toaster.getBidList();
            }

            //?
            void readBrokeType()
            {

            }

            //gets the current stock price
            void setCurrentPrice(double p)
            {
                if (p>0) 
                {
                    currentPrice=p/10000.0;
                    updatePriceStats();
                }
            }

            //data accessors

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

            //creates statistics needed by a SIBI based algorithms
            void setSOBIStats()
            {   if (DEBUG)
            {
                cout << "bids p*v " << get<0>(currentSOBI) << " bids v " << get<1>(currentSOBI)<<endl;
                cout << "asks p*v " << get<3>(currentSOBI) << " bids b " << get<4>(currentSOBI)<<endl;
            }

            if ( currentPrice>0 && (get<1>(currentSOBI) != 0 || get<4>(currentSOBI) != 0) ) {
                    //this is a hack need a better solution !!!!
                    //creates vwap for asks and bids
                double bids_price=((double)get<0>(currentSOBI))/get<1>(currentSOBI);
                double asks_price=((double)get<3>(currentSOBI))/get<4>(currentSOBI);

                if (bids_price > currentPrice || asks_price < currentPrice)
                {
                    currentPrice=(bids_price+asks_price)/2.0;
                    updatePriceStats();
                }

                    //another ugly hack
                    //in for using a skip not a drop policy on exchange server
                if (asks_price < bids_price) asks_price=bids_price+0.000001;
                if (DEBUG)
                {
                    cout << "CPrice " <<currentPrice << " bids p*v/v " <<bids_price<<endl;
                    cout << "CPrice " <<currentPrice << " asks p*v/v " <<asks_price<<endl; 
                } 

                    //collects the difference between current price and vwap of bids and asks
                bids_diff= currentPrice - ((double)get<0>(currentSOBI))/get<1>(currentSOBI);
                asks_diff=((double)get<3>(currentSOBI))/get<4>(currentSOBI) - currentPrice;
            }

        }

            //looks at a current price and updates avg and SD of current stock price
        void updatePriceStats()
        {
            boost::mutex::scoped_lock lock(mutex_price);

            numberSamples++;
            double delta = currentPrice - meanPrice;
            meanPrice = meanPrice + delta / numberSamples;
            M2 = M2 + delta * (currentPrice - meanPrice);

            sampleVariance = M2 / numberSamples;
            populationVariance = M2 / (numberSamples - 1);

            if (DEBUG)
                cout<<"CP-"<<currentPrice<<" Mean "<<meanPrice<<" SD "<<sampleVariance<<endl;
        }

            //some stats on timed SOBI not yet functional
        void setSOBITimedStats()
        {
            asksTime=get<0>(currentTimedSOBI);
            bidsTime=get<4>(currentTimedSOBI);
        }
        
        //locks for various functions
        boost::mutex mutex;
        boost::mutex mutex_price;

        ToasterConnection toaster;

        //Statistics of interest
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
        bool DEBUG;

        list<PVpair> askPlays;
        list<PVpair> bidPlays;


    };
}
}
#endif
