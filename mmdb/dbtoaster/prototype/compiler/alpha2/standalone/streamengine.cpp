#include "streamengine.h"

// DBToaster compiler generates init()
// -- adds streams to multiplexer
// -- registers stream handlers
extern void init(DBToaster::multiplexer& sources, DBToaster::dispatcher& router);

int main(int argc, char** argv)
{
    DBToaster::multiplexer sources(12345, 20);
    DBToaster::dispatcher router;
    init(sources, router);

    while ( sources.stream_has_inputs() ) {
        DBToaster::dbtoaster_tuple t = sources.next_input();
        router.dispatch(t);
    }
}
