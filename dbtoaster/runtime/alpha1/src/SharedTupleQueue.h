

#ifndef SHARED_TUPLE_QUEUE_FOR_QUERY
#define SHARED_TUPLE_QUEUE_FOR_QUERY


#include <iostream>
#include <deque>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

#include <boost/ptr_container/ptr_map.hpp>

#include "SharedTuple.h"

//#include <boost/queue.hpp>
//#include <boost/type_traits.hpp>

namespace DBToasterRuntime{

	using namespace std;
	using namespace boost;
	using namespace boost::interprocess;

	class SharedTupleQueue
	{
	public:

		SharedTupleQueue()
		{
			boost::mutex::scoped_lock  lock(globalLock);

			numberOfQueries=0;                   //total amount of queries connected
			numberOfReaders=0;

		}

		SharedTupleQueue(int queryID)
		{
		//needs a locks everything to change structures
		//TODO: check if absolutely needed;
			boost::mutex::scoped_lock  lock(globalLock);

			positionsInQ[queryID]=0;
			numberOfQueries=1;                   //total amount of queries connected
			numberOfReaders=0;                   //current amount of queries which are reading the data
		}

		SharedTupleQueue* join(int queryID)
		{
			boost::mutex::scoped_lock  lock(globalLock);

			positionsInQ[queryID]=0;
			numberOfQueries++;
			return this;
		}

		void addTuple(void* tuple)
		{
		//writer needs to lock all the structures so no one else can do reads or writes
			boost::mutex::scoped_lock  writeLK(writeRequest);
			boost::mutex::scoped_lock  lock(globalLock);

			SharedTuple *node=new SharedTuple();
			node->queryAccesses=0;
			node->tuple=tuple;
			data.push_back(node);
		}

		void * getTuple(int queryID)
		{
			void * returnValue;

		//reads data from queue in a proper place
			boost::mutex::scoped_lock  writeLK(writeRequest);		
			boost::mutex::scoped_lock  lock(readLock);
				numberOfReaders++;
				if ( numberOfReaders == 1 )
				{
					globalLock.lock();
				}
			lock.unlock();
			writeLK.unlock();

			int offset=positionsInQ[queryID];
			deque<SharedTuple*>::iterator currentPos=data.begin();
			int i=0; 
			//have to do this manually it does not support +int
			while (currentPos != data.end() && i < offset)
			{
				currentPos++;
				i++;			
			}

			if (currentPos == data.end())
			{
				returnValue=NULL;
			}
			else
			{
				SharedTuple * item=*currentPos;
				(*currentPos)->queryAccesses++;
				positionsInQ[queryID]++;

				returnValue=item->tuple;
			}

			lock.lock();
				numberOfReaders--;
				if ( numberOfReaders == 0 )
				{
					globalLock.unlock();
					//notify the writeres
				}
			lock.unlock();

			return returnValue;
		}

		bool isEmpty(int queryID)
		{
			boost::mutex::scoped_lock  lock(globalLock);
		
			if (!data.empty())
			{
				if ( positionsInQ[queryID] == data.size() )
				{
					return true;
				}
				else
				{
					return false;
				}
			}
			else
			{
				return true;
			}
		}

		void removeQuery(int queryID)
		{
			boost::mutex::scoped_lock  lock(globalLock);

			deque<SharedTuple*>::iterator item=data.begin();
			for (int i=0; i < positionsInQ[queryID]; i++)
			{
				(*item)->queryAccesses--;
				item++;
			}

			positionsInQ.erase(queryID);
			numberOfQueries--;

		}

		void cleanTuples()
		{
			boost::mutex::scoped_lock  lock(globalLock);
			
			int offset=0;
			deque<SharedTuple*>::iterator currentPos=data.begin();

			while ( currentPos != data.end() && (*currentPos)->queryAccesses == numberOfQueries )
			{
				currentPos++;
				offset++;
			}
			
			for(int i=0; i < offset; i++){
				data.pop_front();
			}
			
			map<int, int>::iterator pos=positionsInQ.begin();		
			for (; pos != positionsInQ.end(); pos++)
			{
				pos->second=(pos->second)-offset;
			}

		}

	private:

//	ptr_map<int, deque<SharedTuple*>::iterator>  queryPosition;
		map<int, int>                                positionsInQ;
		deque<SharedTuple*>                          data;
		int                                          numberOfQueries;
		int                                          numberOfReaders;
		int                                          minPosition;
		boost::mutex                                 readLock;
		boost::mutex                                 globalLock;
		boost::mutex							     writeRequest;
		boost::condition_variable                    writerWaite;

	};
};

#endif