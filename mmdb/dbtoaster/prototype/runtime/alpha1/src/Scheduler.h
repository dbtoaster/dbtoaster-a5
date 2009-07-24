
#ifndef SCHEDULER_FOR_QUERY_EXECUTOR
#define SCHEDULER_FOR_QUERY_EXECUTOR

#include <list>
#include <map>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

#include "RunableQueryQueue.h"
#include "InputBufferInterface.h"
#include "SchedulerInterface.h"

namespace DBToasterRuntime
{

	using namespace std;
	using namespace boost;

//class InputBuffer;

	class Scheduler: public SchedulerInterface
	{
	public:

		Scheduler()
		{
			//TODO: initialize this two with data 
			// can be done in the catalog ...
			sourceToQuery;
			quryToQuerySources;

		}	
		void addInputBuffer(InputBufferInterface* data)
		{
			readyData=data;
		}	
		void checkAndAdd(int & querySourceID)
		{
			boost::mutex::scoped_lock  lock(queriesLock);
			
			if ( !availableQueries.isActive(sourceToQuery[querySourceID]) )
			{
				availableQueries.addQuery(sourceToQuery[querySourceID]);
			}
		}

		void execution(int numThreads)
		{
		//create a set of threads with runThread and their main function.
			runThread();

		}

	private:
		void runThread()
		{
			int queryID=availableQueries.nextQuery();
			//void* tuples=readyData->getTuple(queryID);
			list<int>::iterator querySource=quryToQuerySources[queryID].begin();
			for (; querySource != quryToQuerySources[queryID].end(); querySource++)
			{
				
				if (readyData->hasNext(*querySource))
				{
					void* tuple=readyData->getTuple(*querySource);
					runQuery(tuple, queryID);

					readyData->removeTuples(*querySource);

				}
				
			}
			boost::mutex::scoped_lock  lock(queriesLock);
			bool toAdd=false;
			querySource=quryToQuerySources[queryID].begin();
			list<int>::iterator stop=quryToQuerySources[queryID].end();
			for (; (querySource != stop) && !toAdd; querySource++)
			{
				if (readyData->hasNext(queryID))
				{
					toAdd=true;
				}
			}
			if (!toAdd)
			{
				availableQueries.removeQuery(queryID);
			}
			lock.unlock();
		}
			void runQuery(void* tuple, int & queryID)
			{
	 	     //get handle from a catalog
		     //pass in tuples
		     //get results
		     //put results into QueryResultsQueue
			}


		//  QueryCatalog             queryHandles;
			map<int, int>            sourceToQuery;
			map<int, list<int> >     quryToQuerySources;
			InputBufferInterface*    readyData;
			RunableQueryQueue        availableQueries;
			boost::mutex             queriesLock;

		};
	};
#endif