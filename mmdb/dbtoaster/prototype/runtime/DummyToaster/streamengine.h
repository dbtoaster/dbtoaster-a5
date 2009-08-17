#ifndef DBTOASTER_COMMON_H
#define DBTOASTER_COMMON_H

#include <iostream>
#include <list>
#include <map>
#include <vector>

#include <tr1/tuple>

#include <boost/any.hpp>
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
        
        class Multiplexer : public ClientStream<boost::any>
        {
          public:
              Multiplexer()
              {
                  position=0;
              }
              
              void read(  boost::function<void ()> handler )
              {
                  deque< ClientStream<boost::any>* >::iterator stream=data.begin();
                  for (; stream != data.end(); stream++)
                  {
                      stream->read(boost::bind(&addToQueue(stream)));
                  }
                  
                  dispatch();
                  
                  handler();
                  
              }
              
              void dispatch()
              {
                  AnyClientStream* stream;
                  boost::mutex::scoped_lock lock(mutex);
                  
                    if (readyQueue.empty())
                    {
                        conditionVar.wait(lock);
                    }
                    
                    stream = readyQueue.front();
                    readyQueue.pop_front();
                  
                  lock.unlock();
                  
                  stream->dispatch();
                  
              }
              
              void addToQueue(AnyClientStream* s)
              {
                  boost::mutex::scoped_lock lock(mutex);
                  
                    readyQueue.push_back(s);
                  
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
                          item=readyQueue.erase();
                      }
                      else
                      {
                          item++;
                      }
                  }
                  
                  data.erase(stream);
                  
                  map<ClientStreamID, int>::iterator pos=idToPosition.begin();
                  int counter=1;
                  while (pos != idToPosition.end)
                  {
                      if ((*pos).second > currentPos)
                      {
                          (*pos).second--; 
                      }
                      
                      if ((*pos).first == id)
                      {
                          pos=idToPosition.erase();
                      }
                      else
                      {
                          pos++;
                      }
                  }
                  
              }
              
          private:
              
              map<ClientStreamID, int> idToPosition;
              int position;
              
              deque<AnyClientStream*> readyQueue;
              
              boost::mutex mutex;
              boost::condition_variable conditionVar;
              
                    
        };

        

        typedef struct
        {
            DmlType type;
            DBToasterStreamId id;
            boost::any data;
        } DBToasterTuple;

        template<typename tuple>
        struct DBToasterStream : public ClientStream<boost::any>
        {
            ClientStream<tuple>* stream;
            DBToasterStream(ClientStream<tuple>* s) : stream(s) {}
            inline void read(boost::function<void ()> handler) { stream->read(handler); }
            inline void dispatch() {stream->dispatch()};
//            inline void initStream() { stream->initStream(); }
//            inline bool streamHasInputs() { return stream->streamHasInputs(); }
//            inline boost::any nextInput() { return boost::any(stream->nextInput()); }
//            inline unsigned int getBufferSize() { return stream->getBufferSize(); }
            bool operator==(const ClientStream<tuple>*& other) const { return stream == other; }
        };

        struct Multiplexer : public ClientStream<DBToasterTuple>
        {
            typedef boost::function<void(DBToasterTuple&, boost::any&)> TupleAdaptor;
            typedef ClientStream<boost::any> my_AnyStream;
            typedef shared_ptr< my_AnyStream > Shared_ptr_Stream;
            vector< Shared_ptr_Stream > inputs;
            vector<TupleAdaptor> inputAdaptors;
            map< shared_ptr<AnyStream>, DBToasterStreamId> inputIds;
            int streamStep;
            int numStreams;
            
//            boost::asio::io_service& localIOService;

            tuple<int, int> runningStream;
            

            Multiplexer(int seed, int stepsize)
                : streamStep(stepsize)
            {
                
            }

            template<typename tuple>
                void addStream(ClientStream<tuple>* s, TupleAdaptor a, DBToasterStreamId id)
            {
                shared_ptr< DBToasterStream<tuple> > dbts(new DBToasterStream<tuple>(s));
                inputs.push_back(dbts);
                inputAdaptors.push_back(a);
                inputIds[dbts] = id;
                numStreams = inputs.size();
            }

            template<typename tuple>
            void removeStream(Stream<tuple>* s)
            {
                vector< shared_ptr<AnyStream> >::iterator sIt = inputs.begin();
                vector< shared_ptr<AnyStream> >::iterator sEnd = inputs.end();
                for (unsigned int i = 0; sIt != sEnd; ++sIt, ++i) {
                    shared_ptr< DBToasterStream<tuple> > os =
                        dynamic_pointer_cast< DBToasterStream<tuple>, AnyStream >(*sIt);

                    if ( os && (*os) == s ) {
                        inputIds.erase(*sIt);
                        inputAdaptors.erase(i);
                        inputs.erase(sIt);
                        break;
                    }
                }

                numStreams = inputs.size();
            }

            void initStream()
            {
                vector< shared_ptr<AnyStream> >::const_iterator sIt = inputs.begin();
                vector< shared_ptr<AnyStream> >::const_iterator sEnd = inputs.end();

                for (; sIt != sEnd; ++sIt) (*sIt)->initStream();
            }

//            bool streamHasInputs()
//            {
//                return !(inputs.empty());
//            }

            void read( boost::function<void ()> handler )
            {
//                assert ( streamHasInputs() );

                int currentStream = currentStream = (int) (numStreams * (rand() / (RAND_MAX + 1.0)));//get<0>(runningStream);
//                int remainingCount = get<1>(runningStream);

//                while ( remainingCount == 0 ) {
//                    currentStream = (int) (numStreams * (rand() / (RAND_MAX + 1.0)));
//                    remainingCount = (int) (streamStep * (rand() / (RAND_MAX + 1.0)));
//                }

                shared_ptr<AnyStream> s = inputs[currentStream];
//                TupleAdaptor adaptor = inputAdaptors[currentStream];

//                DBToasterTuple rTuple;
                s->read(handler);
//                boost::any nextTuple = s->nextInput();
//                adaptor(rTuple, nextTuple);
//                rTuple.id = inputIds[s];

//                if ( !s->streamHasInputs() )
//                {
//                    cout << "Done with stream " << currentStream << endl;
//                    inputs.erase(inputs.begin()+currentStream);
//                    --numStreams;

//                    remainingCount = 0;
//                }
//                else
//                    --remainingCount;

//                runningStream = make_tuple(currentStream, remainingCount);
//                return rTuple;
            }

/*            unsigned int getBufferSize()
            {
                vector< shared_ptr<AnyStream> >::iterator insIt = inputs.begin();
                vector< shared_ptr<AnyStream> >::iterator insEnd = inputs.end();

                unsigned int r = 0;
                for (; insIt != insEnd; ++insIt)
                    r += (*insIt)->getBufferSize();

                return r;
            }
            */
        };

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
