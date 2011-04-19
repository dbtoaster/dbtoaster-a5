
#ifndef INPUT_BUFFER_RUNTIME
#define INPUT_BUFFER_RUNTIME

#include <list>
#include <map>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/ptr_container/ptr_list.hpp>
#include <tr1/unordered_map>

#include "SharedTupleQueue.h"
#include "InputBufferInterface.h"
#include "SchedulerInterface.h"

namespace DBToasterRuntime
{
	using namespace std;
	using namespace tr1;

	class InputBuffer: public InputBufferInterface 
	{ 
	public:
		InputBuffer()
		{

		}

		void addScheduler(SchedulerInterface* sch)
		{
			schedulingQueue=sch;
		}

		void addTuple(int & sourceID, void* structuredTuple)
		{
			SourceQueueMap::iterator queuePtr=sourceAccess.find(sourceID);
			if ( queuePtr == sourceAccess.end() )
			{
				//if the queue doesn't exist it means that there is no queries to run on the data
				return ;
			}
			sourceAccess[sourceID]->addTuple(structuredTuple);
			
			//checks if a query needs to be added to the scheduler if so add
			list<int>::iterator queries=availableQueries[sourceID].begin();
			for (; queries != availableQueries[sourceID].end(); queries++)
			{
				if ( sourceAccess[sourceID]->isEmpty(*queries) )
				{
					schedulingQueue->checkAndAdd(*queries);
				}
			}
		}

		void* getTuple(int & queryID)
		{
			//caution returns NULL if non exist
			
			void * output=NULL;
			QueryQueueMap::iterator queuePtr=queryAccess.find(queryID);
			if ( queuePtr == queryAccess.end() ) {
			//This is an error there must be a queue
			//TODO: throw an error
				return output;
			}
			else
			{			
				output=(*queuePtr).second->getTuple(queryID);
				
			}
		}

		void addQuery(int & queryID, int & sourceID)
		{
			SourceQueueMap::iterator queuePtr=sourceAccess.find(sourceID);
			if ( queuePtr == sourceAccess.end() ) 
			{
				sourceAccess[sourceID]=new SharedTupleQueue(queryID);
				
				list<int> queries;
				queries.push_back(queryID);
				availableQueries[sourceID]=queries;
			}
			else
			{
				sourceAccess[sourceID]->join(queryID);
				availableQueries[sourceID].push_back(queryID);
			}
			
			queryAccess[queryID]=sourceAccess[sourceID];
			
/*			list<SharedTupleQueue*> sources;
			list<int>::iterator item=sourceIDs.begin();
			for (; item != sourceIDs.end(); item++)
			{
				SourceQueueMap::iterator queuePtr=sourceAccess.find(*item);
				if ( queuePtr == sourceAccess.end() ) 
				{
					sourceAccess[*item]=new SharedTupleQueue(queryID);
					
					list<int> queries;
					queries.push_back(queryID);
					availableQueries[*item]=queries;
				}
				else
				{
					sourceAccess[*item]->join(queryID);
					availableQueries[*item].push_back(queryID);
				}
				
				sources.push_back(sourceAccess[*item]);
			}
			queryAccess[queryID]=sources;
			*/
		}

		void removeTuples(int & queryID)
		{
			SharedTupleQueue* sourceQueue=queryAccess[queryID];
			sourceQueue->cleanTuples();
		}

		void removeQuery(int & queryID)
		{
			QueryQueueMap::iterator queuePtr=queryAccess.find(queryID);
			if (queuePtr == queryAccess.end())
			{
			//nothing to do query is not there
				return ;
			}
			else
			{
				(*queuePtr).second->removeQuery(queryID);
				queryAccess.erase(queryID);			
			}
		}

		bool hasNext(int & queryID)
		{
			bool next=true;
			QueryQueueMap::iterator queuePtr=queryAccess.find(queryID);
			if (queuePtr == queryAccess.end())
			{
				//nothing to do query is not there
				return false;
			}
			else
			{
				next=(*queuePtr).second->isEmpty(queryID);
			}
			return next;
		}
		 


	private:
		typedef unordered_map<int, SharedTupleQueue*>   SourceQueueMap;
		typedef map<int, SharedTupleQueue* >            QueryQueueMap;
//  TODO: ADD Scheduler and MemoryManager
//	MemoryManager Jim; 
		SchedulerInterface*        schedulingQueue; 
		SourceQueueMap             sourceAccess; 
		QueryQueueMap              queryAccess; 
		map<int, list<int> >       availableQueries; 
	};

};

#endif