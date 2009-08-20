#ifndef DBTOASTER_COMMON_H
#define DBTOASTER_COMMON_H

#include <iostream>
#include <list>
#include <map>
#include <vector>

#include <tr1/tuple>

#include <boost/any.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "dataclient.h"

namespace DBToaster
{
    namespace StandaloneEngine
    {
        using namespace std;
        using namespace tr1;
        using namespace boost;

        using namespace DBToaster::DemoDatasets;

        enum DmlType { insertTuple, deleteTuple };

        typedef ClientStream<boost::any> AnyClientStream;
        typedef int ClientStreamID;

        class Multiplexer : public ClientStream<AnyClientStream>
        {
        public:

            Multiplexer()
            {
                position=0;
                firstRead=true;
                continuousReading=true;
            }
            
            void read (  boost::function<void ()> handler )
            {
                boost::mutex::scoped_lock lock(mutex);

                if (readyQueue.empty()){

                    firstRead=false;

                    int i=0; 
                    deque< AnyClientStream* >::iterator stream=data.begin();
                    for (; stream != data.end(); stream++)
                    {
                        (*stream)->read( boost::bind( &Multiplexer::addToQueue, this, (*stream) ) );
                    }
                }
                else
                {
                    dispatch();
                }
            }

            void dispatch()
            {
                AnyClientStream* stream;
                bool doRead=false;
                
                boost::mutex::scoped_lock lock(mutex);

                if (readyQueue.empty())
                {
                    cout<<"Error occured Multiplexer::dispatch doesn't have any data"<<endl;
//                          conditionVar.wait(lock);
                }

                stream = readyQueue.front();
                readyQueue.pop_front();
                
                if (continuousReading || numberReads>=0)
                {
                    if (!continuousReading) numberReads--;
                    doRead=true;
                }

                lock.unlock();

                stream->dispatch();
                
                if (doRead)
                {
                    read(boost::bind(&Multiplexer::doNothing, this));
                }

            }
            
            void setNumberReads(int n)
            {
                continuousReading=false;
                numberReads=n;
            }
            
            void setContinuousReading()
            {
                continuousReading=true;
            }

            void doNothing()
            {
                  //for now just dummy functoin;
            }

            void addToQueue(AnyClientStream* s)
            {
                bool doDispatch=false;

                boost::mutex::scoped_lock lock(mutex);

                if (!firstRead)
                {
                    firstRead=true;
                    doDispatch=true;
                }

                readyQueue.push_back(s);

                lock.unlock();

                if (doDispatch) dispatch();
                

            }        
            
            void addStream(AnyClientStream* s, ClientStreamID id)
            {
                boost::mutex::scoped_lock lock(mutex);

                position++;
                idToPosition[id]=position;

                data.push_back(s);

            }

            void removeStream(ClientStreamID id)
            {
                boost::mutex::scoped_lock lock(mutex);

                int currentPos=idToPosition[id];

                deque<AnyClientStream*>::iterator stream=data.begin();
                for (int i=0; i < currentPos ; i++)
                {
                    stream++;
                }

                deque<AnyClientStream*>::iterator item=readyQueue.begin();
                while (item != readyQueue.end())
                {
                    if ((*item) == (*stream))
                    {
                        item=readyQueue.erase(item);
                    }
                    else
                    {
                        item++;
                    }
                }

                data.erase(stream);

                map<ClientStreamID, int>::iterator pos=idToPosition.begin();
                int counter=1;
                while ( pos != idToPosition.end() )
                {
                    if ((*pos).second > currentPos)
                    {
                        (*pos).second--; 
                    }

                }
                idToPosition.erase(id);

            }

        private:

            map<ClientStreamID, int> idToPosition;
            int position;

            int numberReads;
            bool firstRead;
            bool continuousReading;

            deque<AnyClientStream*> readyQueue;

            boost::mutex mutex;
            boost::condition_variable conditionVar;

        };

        //ends


        typedef int DBToasterStreamId;

        typedef struct
        {
            DmlType type;
            DBToasterStreamId id;
            boost::any data;
        } DBToasterTuple;

            struct Dispatcher
        {
            typedef boost::function<void (boost::any)> Handler;
            typedef tuple<DBToasterStreamId, DmlType> Key;
            typedef map<Key, Handler> HandlerMap;
            HandlerMap handlers;

            Dispatcher() {}

            void addHandler(DBToasterStreamId streamId, DmlType type, Handler h)
            {
                Key k = make_tuple(streamId, type);
                if ( handlers.find(k) == handlers.end() )
                    handlers[k] = h;
                else {
                    cout << "Found existing " << (type == insertTuple? "insert" : "delete")
                        << " handler for stream id " << streamId << endl;
                }
            }

            void removeHandler(DBToasterStreamId streamId, DmlType type)
            {
                Key k = make_tuple(streamId, type);
                HandlerMap::iterator hIt = handlers.find(k);
                if ( hIt != handlers.end() )
                    handlers.erase(hIt);
                else {
                    cout << "Could not find " << (type == insertTuple? "insert" : "delete")
                        << " handler for stream id " << streamId << endl;
                }
            }

            void dispatch(DBToasterTuple& tuple)
            {
                Key k = make_tuple(tuple.id, tuple.type);
                HandlerMap::iterator hIt = handlers.find(k);
                if ( hIt != handlers.end() ) {
                    (hIt->second)(tuple.data);
                }
            }
        };
    };
};

#endif
