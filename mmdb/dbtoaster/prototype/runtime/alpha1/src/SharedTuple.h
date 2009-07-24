
#ifndef SHARED_TUPLE_FOR_SHARED_QUEUE
#define SHARED_TUPLE_FOR_SHARED_QUEUE

namespace DBToasterRuntime
{
	struct SharedTuple
	{

		int queryAccesses;
		void * tuple;

	};
};

#endif
