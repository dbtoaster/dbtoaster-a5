#include "streamengine.h"

// DBToaster compiler generates init()
// -- adds streams to multiplexer
// -- registers stream handlers
extern void init(DBToaster::StandaloneEngine::FileMultiplexer& sources, DBToaster::StandaloneEngine::Dispatcher& router);

int main(int argc, char** argv)
{
    DBToaster::StandaloneEngine::FileMultiplexer sources(12345, 20);
    DBToaster::StandaloneEngine::Dispatcher router;
    init(sources, router);

    while ( sources.streamHasInputs() ) {
        DBToaster::StandaloneEngine::DBToasterTuple t = sources.nextInput();
        router.dispatch(t);
    }
}
