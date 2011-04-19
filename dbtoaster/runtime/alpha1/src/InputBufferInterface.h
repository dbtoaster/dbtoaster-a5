

#ifndef INPUT_BUFFER_INTERFACE_RUNTIME
#define INPUT_BUFFER_INTERFACE_RUNTIME

#include <list>

namespace DBToasterRuntime
{
	using namespace std;
	
	class InputBufferInterface
	{
	public:
			InputBufferInterface()
			{

			}

			virtual void addTuple(int & sourceID, void* structuredTuple)
			{

			}

			virtual void* getTuple(int & queryID)
			{
				return NULL;
			}

			virtual void addQuery(int & queryID, int & sourceIDs)
			{
	
			}

			virtual void removeTuples(int & queryID)
			{

			}

			virtual void removeQuery(int & queryID)
			{
				
			}
			
			virtual bool hasNext(int & queryID)
			{

			}
	
	};
};


#endif;