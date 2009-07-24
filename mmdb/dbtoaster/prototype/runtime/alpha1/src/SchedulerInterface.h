


#ifndef SCHEDULER_INTERFACE_RUNTIME
#define SCHEDULER_INTERFACE_RUNTIME

namespace DBToasterRuntime
{

	class SchedulerInterface{
	public:
		SchedulerInterface()
		{

		}		
		virtual void checkAndAdd(int & queryID)
		{

		}

		virtual void execution(int & numThreads)
		{

		}
	private:
	};
};	

#endif