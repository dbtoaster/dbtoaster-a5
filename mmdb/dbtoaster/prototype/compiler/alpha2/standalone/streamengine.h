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

#include "datasets/datasets.h"
#include "datasets/filesources.h"
#include "datasets/networksources.h"

namespace DBToaster
{
    namespace StandaloneEngine
    {
        using namespace std;
        using namespace tr1;
        using namespace boost;

        using boost::shared_ptr;

        using namespace DBToaster::DemoDatasets;

        enum DmlType { insertTuple, deleteTuple };

        typedef int DBToasterStreamId;

        typedef struct
        {
            DmlType type;
            DBToasterStreamId id;
            boost::any data;
        } DBToasterTuple;


        /////////////////////////
        //
        // File source multiplexing

        template<typename tuple>
        struct DBToasterStream : public FileStream<boost::any>
        {
            FileStream<tuple>* stream;
            DBToasterStream(FileStream<tuple>* s) : stream(s) {}
            inline void initStream() { stream->initStream(); }
            inline bool streamHasInputs() { return stream->streamHasInputs(); }
            inline boost::any nextInput() { return boost::any(stream->nextInput()); }
            inline unsigned int getBufferSize() { return stream->getBufferSize(); }
            bool operator==(const FileStream<tuple>*& other) const { return stream == other; }
        };

        struct FileMultiplexer : public FileStream<DBToasterTuple>
        {
            typedef boost::function<void(DBToasterTuple&, boost::any&)> TupleAdaptor;
            typedef FileStream<boost::any> AnyFileStream;
            vector< shared_ptr<AnyFileStream> > inputs;
            vector<TupleAdaptor> inputAdaptors;
            map< shared_ptr<AnyFileStream>, DBToasterStreamId> inputIds;
            int streamStep;
            int numStreams;

            tuple<int, int> runningStream;
            int init_runningStream;

            FileMultiplexer(int seed, int stepsize) : streamStep(stepsize), init_runningStream(0) {}

            template<typename tuple>
            void addStream(FileStream<tuple>* s, TupleAdaptor a, DBToasterStreamId id)
            {
                shared_ptr< DBToasterStream<tuple> > dbts(new DBToasterStream<tuple>(s));
                inputs.push_back(dbts);
                inputAdaptors.push_back(a);
                inputIds[dbts] = id;
                numStreams = inputs.size();
            }

            template<typename tuple> void removeStream(FileStream<tuple>* s)
            {
                vector< shared_ptr<AnyFileStream> >::iterator sIt = inputs.begin();
                vector< shared_ptr<AnyFileStream> >::iterator sEnd = inputs.end();
                for (unsigned int i = 0; sIt != sEnd; ++sIt, ++i) {
                    shared_ptr< DBToasterStream<tuple> > os =
                        dynamic_pointer_cast< DBToasterStream<tuple>, AnyFileStream >(*sIt);

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
                vector< shared_ptr<AnyFileStream> >::const_iterator sIt = inputs.begin();
                vector< shared_ptr<AnyFileStream> >::const_iterator sEnd = inputs.end();

                for (; sIt != sEnd; ++sIt) (*sIt)->initStream();
            }

            bool streamHasInputs()
            {
                return !(inputs.empty());
            }

            DBToasterTuple nextInput()
            {
                assert ( streamHasInputs() );

                if(init_runningStream == 0) {
                	runningStream = make_tuple( (int) (numStreams * (rand() / (RAND_MAX+1.0))), 
				    (int) (streamStep * (rand() / (RAND_MAX+1.0))));
                    init_runningStream = 1;
                }

                int currentStream = get<0>(runningStream);
                int remainingCount = get<1>(runningStream);

                while (remainingCount == 0) {
                    currentStream = (int) (numStreams * (rand() / (RAND_MAX + 1.0)));
                    remainingCount = (int) (streamStep * (rand() / (RAND_MAX + 1.0)));
                } 

                shared_ptr<AnyFileStream> s = inputs[currentStream];
                TupleAdaptor adaptor = inputAdaptors[currentStream];

                DBToasterTuple rTuple;
                boost::any nextTuple = s->nextInput();
                adaptor(rTuple, nextTuple);
                rTuple.id = inputIds[s];

                if ( !s->streamHasInputs() )
                {
                    cout << "Done with stream " << currentStream << endl;
                    inputs.erase(inputs.begin()+currentStream);
                    --numStreams;

                    remainingCount = 0;
                }
                else
                    --remainingCount;

                runningStream = make_tuple(currentStream, remainingCount);
                return rTuple;
            }

            unsigned int getBufferSize()
            {
                vector< shared_ptr<AnyFileStream> >::iterator insIt = inputs.begin();
                vector< shared_ptr<AnyFileStream> >::iterator insEnd = inputs.end();

                unsigned int r = 0;
                for (; insIt != insEnd; ++insIt)
                    r += (*insIt)->getBufferSize();

                return r;
            }
        };


        ///////////////////////////////
        //
        // File stream dispatching

        struct FileStreamDispatcher
        {
            typedef boost::function<void (boost::any)> Handler;
            typedef tuple<DBToasterStreamId, DmlType> Key;
            typedef map<Key, Handler> HandlerMap;
            HandlerMap handlers;

            FileStreamDispatcher() {}

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


        ///////////////////////////////
        //
        // Network source multiplexing

        class SocketMultiplexer : public SocketStream
        {
        public:

            SocketMultiplexer()
            {
                firstRead=true;
                continuousReading=true;
            }

            void read (  boost::function<void ()> handler )
            {
                boost::mutex::scoped_lock lock(mutex);

                if (readyQueue.empty())
                {
                    firstRead=false;

                    deque<SocketStream*>::iterator stream = multiplexedStreams.begin();
                    for (; stream != multiplexedStreams.end(); stream++)
                    {
                        (*stream)->read(boost::bind(
                            &SocketMultiplexer::addToQueue, this, (*stream) ) );
                    }
                }
                else
                {
                    dispatch();
                }
            }

            void dispatch()
            {
                SocketStream* stream;
                bool doRead = false;

                boost::mutex::scoped_lock lock(mutex);

                if (readyQueue.empty())
                {
                    cout<<"Error occurred:"
                        << " SocketMultiplexer::dispatch doesn't have any data"
                        << endl;
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
                    read(boost::bind(&SocketMultiplexer::doNothing, this));
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
                  //for now just dummy function;
            }

            void addToQueue(SocketStream* s)
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

            void addStream(SocketStream* s)
            {
                boost::mutex::scoped_lock lock(mutex);
                multiplexedStreams.push_back(s);
            }

            void removeStream(SocketStream* s)
            {
                boost::mutex::scoped_lock lock(mutex);
                deque<SocketStream*>::iterator streamFound =
                    find(multiplexedStreams.begin(), multiplexedStreams.end(), s);

                if ( streamFound != multiplexedStreams.end() )
                {
                    deque<SocketStream*>::iterator readyStreamFound =
                        find(readyQueue.begin(), readyQueue.end(), s);

                    if ( readyStreamFound != readyQueue.end() ) {
                        readyQueue.erase(readyStreamFound);
                    }
                    multiplexedStreams.erase(streamFound);
                }
                else
                {
                    cout << "Attempted to remove a non-existent stream." << endl;
                }
            }

        private:
            int numberReads;
            bool firstRead;
            bool continuousReading;

            deque<SocketStream*> multiplexedStreams;
            deque<SocketStream*> readyQueue;

            boost::mutex mutex;
            boost::condition_variable conditionVar;
        };
    }
}

#endif
