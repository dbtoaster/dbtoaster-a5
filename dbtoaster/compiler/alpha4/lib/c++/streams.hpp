#ifndef DBTOASTER_STREAMS_H
#define DBTOASTER_STREAMS_H

#include <vector>
#include <boost/any.hpp>

namespace dbtoaster {
  using namespace ::std;
  using namespace ::boost;

  enum stream_event_type { insert_tuple, delete_tuple };

  typedef int stream_id;
  typedef vector<boost::any> event_data;

  struct stream_event
  {
    stream_event_type type;
    stream_id id;
    event_data data;
    stream_event(stream_event_type t, stream_id i, event_data& d)
      : type(t), id(i), data(d)
    {}
  };
}

#endif

