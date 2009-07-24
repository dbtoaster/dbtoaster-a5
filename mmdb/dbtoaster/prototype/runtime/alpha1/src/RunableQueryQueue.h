

#ifndef RUNABLE_QUERY_QUEUE_FOR_SHCEDULER
#define RUNABLE_QUERY_QUEUE_FOR_SHCEDULER

#include <list>
#include <map>

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>


namespace DBToasterRuntime
{
	using namespace std;
	using namespace boost;


	class RunableQueryQueue
	{

	public:	
		RunableQueryQueue()
		{

		}

		int   nextQuery()
		{
			boost::mutex::scoped_lock  lock(globalLock);
			
			while( readyQueries.empty() )
			{
				readWait.wait(lock);
			}	
				
			int returnValue=readyQueries.front();
			readyQueries.pop_front();
			return returnValue;
		}

		void   addQuery(int queryID)
		{
			boost::mutex::scoped_lock  lock(globalLock);
			
			readyQueries.push_back(queryID);
			activeQueries[queryID]=queryID;
			
			lock.unlock();
			readWait.notify_one();

		}
		void   removeQuery(int queryID)
		{
			boost::mutex::scoped_lock  lock(globalLock);
			activeQueries.erase(queryID);
		}
		
		bool   isActive(int queryID)
		{
			boost::mutex::scoped_lock  lock(globalLock);
			
			if ( queryID == activeQueries[queryID] )
			{
				return true;
			}
			else
			{
				return false;
			}

		}
		
		bool isEmpty()
		{
			boost::mutex::scoped_lock  lock(globalLock);
			
			return readyQueries.empty();
		}

		private:

			list<int>                   readyQueries;
			map<int, int>               activeQueries;
			boost::mutex                globalLock;
			boost::condition_variable   readWait;

		};
	};
#endif