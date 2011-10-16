#ifndef DBTOASTER_STREAMS_H
#define DBTOASTER_STREAMS_H

#include <map>
#include <set>
#include <vector>
#include <sys/time.h>
#include <boost/any.hpp>
#include <boost/filesystem.hpp>
#include <boost/function.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/lambda/lambda.hpp>
#include <boost/lambda/bind.hpp>

namespace dbtoaster {
  namespace streams {
    using namespace ::std;
    using namespace ::boost;
    using namespace boost::filesystem;
    using namespace boost::iostreams;
    using namespace boost::lambda;

    enum stream_event_type { delete_tuple, insert_tuple };

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

    struct ordered {
      virtual unsigned int order() = 0;
    };

    // Adaptor and stream interfaces.
    struct stream_adaptor : public ordered
    {
      // All adaptors have an internal ordering for input stream synchronization.
      unsigned int current_order;
      stream_adaptor() : current_order(0) {}

      // The default ordering function.
      virtual unsigned int order() { return current_order; }

      // processes the data, adding all stream events generated to the list.
      virtual void process(const string& data,
                           shared_ptr<list<stream_event> > dest) = 0;

      virtual void finalize(shared_ptr<list<stream_event> > dest) = 0;
    };

    // Framing
    enum frame_type { fixed_size, delimited, variable_size };
    struct frame_descriptor {
      frame_type type;
      int size;
      string delimiter;
      int off_to_size;
      int off_to_end;
      frame_descriptor() : type(delimited), delimiter("\n") {}
      frame_descriptor(string d) : type(delimited), delimiter(d) {}
      frame_descriptor(int sz) : type(fixed_size), size(sz) {}
      frame_descriptor(int os, int oe)
        : type(variable_size), off_to_size(os), off_to_end(oe)
      {}
    };

    struct dynamic_poset {
      typedef set<shared_ptr<ordered> > pset;
      typedef map<unsigned int, shared_ptr<pset> > repr;
      typedef repr::iterator iterator;
      typedef pair<pset::iterator, pset::iterator> class_range;
      repr poset;

      dynamic_poset() {}

      dynamic_poset(pset& elements) {
        for (pset::iterator it = elements.begin(); it != elements.end(); ++it) {
          add_element(*it);
        }
      }

      inline void clear() { poset.clear(); }
      inline bool empty() { return poset.empty(); }
      inline iterator find(unsigned int o) { return poset.find(o); }
      inline iterator begin() { return poset.begin(); }
      inline iterator end() { return poset.end(); }

      // Returns the total number of elements in the poset.
      size_t size() {
        size_t r = 0;
        for (iterator it = begin(); it != end(); ++it) {
          r += it->second? it->second->size(): 0;
        }
        return r;
      }

      unsigned int order() { return poset.empty()? 0 : poset.begin()->first; }

      shared_ptr<class_range> range(unsigned int order) {
        shared_ptr<class_range> r;
        iterator it = find(order);
        if ( it == end() || !(it->second) ) return r;
        r = shared_ptr<class_range>(new class_range(it->second->begin(), it->second->end()));
        return r;
      }

      void add_element(shared_ptr<ordered> e) {
        if ( !poset[e->order()] ) {
          poset[e->order()] = shared_ptr<pset>(new pset());
          poset[e->order()]->insert(e);
        } else {
          poset[e->order()]->insert(e);
        }
      }

      void remove_element(shared_ptr<ordered> e) {
        iterator it = find(e->order());
        if ( it != end() ) it->second->erase(e);
      }

      // Helper to update an element's position, by removing it from a
      // specific stage.
      void remove_element(unsigned int order, shared_ptr<ordered> e) {
        iterator it = poset.find(order);
        if ( it != poset.end() ) it->second->erase(e);
      }

      void reorder_elements(unsigned int order) {
        iterator it = find(order);
        if ( it == end() ) return;
        shared_ptr<pset> ps = it->second;
        pset removals;

        // Process adaptors, tracking those that have changed their position.
        for(pset::iterator it = ps->begin(); it != ps->end(); ++it) {
          if ( (*it) && (*it)->order() != order ) removals.insert(*it);
        }

        pset::iterator rm_it = removals.begin();
        pset::iterator rm_end = removals.end();
        for (; rm_it != rm_end; ++rm_it) {
          // Re-add the adaptor at its new stage.
          if ( (*rm_it)->order() > order ) add_element(*rm_it);
          else if ( (*rm_it)->order() < order ) {
            cout << "invalid adaptor order ... removing" << endl;
          }
          ps->erase(*rm_it);
        }
        if ( ps->empty() ) poset.erase(order);
      }

    };

    // Sources
    struct source : public ordered
    {
      typedef list<shared_ptr<stream_adaptor> > adaptor_list;
      frame_descriptor frame_info;
      dynamic_poset adaptors;

      source(frame_descriptor& f, adaptor_list& a) : frame_info(f) {
        for(adaptor_list::iterator it = a.begin(); it != a.end(); ++it) {
          add_adaptor(*it);
        }
      }

      unsigned int order() { return adaptors.order(); }

      void add_adaptor(shared_ptr<stream_adaptor> a) {
        adaptors.add_element(dynamic_pointer_cast<ordered>(a));
      }

      void remove_adaptor(unsigned int order, shared_ptr<stream_adaptor> a) {
        adaptors.remove_element(order, dynamic_pointer_cast<ordered>(a));
      }

      virtual void init_source() = 0;
      virtual bool has_inputs() = 0;
      virtual shared_ptr<list<stream_event> > next_inputs() = 0;
    };

    struct dbt_file_source : public source
    {
      typedef stream<file_source> file_stream;
      shared_ptr<file_stream> source_stream;
      shared_ptr<string> buffer;
      dbt_file_source(const string& path, frame_descriptor& f, adaptor_list& a)
        : source(f,a)
      {
        source_stream = shared_ptr<file_stream>(new file_stream(path));
        if ( !source_stream ) {
          cerr << "failed to open file source " << path << endl;
        } else {
          cout << "reading from " << path
               << " with " << a.size() << " adaptors" << endl;
        }
        buffer = shared_ptr<string>(new string());
      }

      void init_source() {}

      bool has_inputs() { return has_frame() || source_stream->good(); }

      bool has_frame() {
        bool r = false;
        if ( frame_info.type == fixed_size ) {
          r = buffer && buffer->size() >= frame_info.size;
        } else if ( frame_info.type == delimited ) {
          r = buffer && (buffer->find(frame_info.delimiter) != string::npos);
        }
        return r;
      }

      shared_ptr<string> frame_from_buffer() {
        shared_ptr<string> r;
        if (frame_info.type == fixed_size) {
          r = shared_ptr<string>(new string(buffer->substr(0,frame_info.size)));
          buffer = shared_ptr<string>(
            new string(buffer->substr(frame_info.size, string::npos)));
        } else if ( frame_info.type == delimited ) {
          size_t delim_pos = buffer->find(frame_info.delimiter);
          r = shared_ptr<string>(new string(buffer->substr(0, delim_pos)));
          buffer = shared_ptr<string>(new string(
            buffer->substr(
              delim_pos+frame_info.delimiter.size(), string::npos)));
        }
        return r;
      }

      shared_ptr<string> next_frame() {
        shared_ptr<string> r;
        char buf[frame_info.size<1024?1024:frame_info.size];
        if (frame_info.type == fixed_size) {
          while ( source_stream->good() && !has_frame() ) {
            source_stream->read(buf, frame_info.size);
            (*buffer) += string(buf, source_stream->gcount());
          }
        }
        else if ( frame_info.type == delimited ) {
          while ( source_stream->good() && !has_frame() ) {
            source_stream->read(buf, sizeof(buf));
            (*buffer) += string(buf, source_stream->gcount());
          }
        }
        else if ( frame_info.type == variable_size ) {
          cerr << "variable size frames not supported" << endl;
        }
        else {
          cerr << "invalid frame type" << endl;
        }
        if ( has_frame() ) r = frame_from_buffer();
        return r;
      }

      // Process adaptors in the first stage, accumulating and returning
      // stream events
      void process_adaptors(string& data, shared_ptr<list<stream_event> >& r) {
        unsigned int min_order = adaptors.order();
        shared_ptr<dynamic_poset::class_range> range = adaptors.range(min_order);
        if ( !range ) {
          cout << "invalid min order at source with empty range" << endl;
          return;
        }

        for(dynamic_poset::pset::iterator it = range->first;
            it != range->second; ++it)
        {
          shared_ptr<stream_adaptor> adaptor =
            dynamic_pointer_cast<stream_adaptor>(*it);
          if ( adaptor ) adaptor->process(data, r);
        }
        adaptors.reorder_elements(min_order);
      }

      // Finalize all adaptors, accumulating stream events.
      void finalize_adaptors(shared_ptr<list<stream_event> >& r) {
        dynamic_poset::iterator it = adaptors.begin();
        dynamic_poset::iterator end = adaptors.end();

        for (; it != end; ++it)
        {
          if ( it->second ) {
            for (dynamic_poset::pset::iterator a_it = it->second->begin();
                 a_it != it->second->end(); ++a_it)
            {
              shared_ptr<stream_adaptor> a =
                dynamic_pointer_cast<stream_adaptor>(*a_it);
              if ( a ) a->finalize(r);
            }
          } else {
            cout << "invalid adaptors poset class at position "
                 << it->first << endl;
          }
        }

        adaptors.clear();
      }

      shared_ptr<list<stream_event> > next_inputs() {
        shared_ptr<list<stream_event> > r;
        if ( adaptors.empty() ) return r;
        if ( has_inputs() ) {
          // get the next frame of data based on the frame type.
          shared_ptr<string> data = next_frame();

          if ( data ) {
            r = shared_ptr<list<stream_event> >(new list<stream_event>());
            process_adaptors(*data, r);
          }
        } else if ( source_stream->is_open() ) {
          source_stream->close();
          r = shared_ptr<list<stream_event> >(new list<stream_event>());
          finalize_adaptors(r);
        }
        return r;
      }
    };

    struct stream_multiplexer
    {
      dynamic_poset inputs;
      shared_ptr<source> current;
      unsigned int current_order;
      int step, remaining, block;

      stream_multiplexer(int seed, int st)
        : current_order(0), step(st), remaining(0), block(100)
      {
        srandom(seed);
      }

      stream_multiplexer(int seed, int st, set<shared_ptr<source> >& s)
      {
        stream_multiplexer(seed, st);
        set<shared_ptr<source> >::iterator it = s.begin();
        set<shared_ptr<source> >::iterator end = s.end();
        for(; it != end; ++it) add_source(*it);
        current_order = inputs.order();
      }

      void add_source(shared_ptr<source> s) {
        inputs.add_element(dynamic_pointer_cast<ordered>(s));
      }

      void remove_source(shared_ptr<source> s) {
        inputs.remove_element(dynamic_pointer_cast<ordered>(s));
      }

      void init_source() {
        dynamic_poset::iterator it = inputs.begin();
        dynamic_poset::iterator end = inputs.end();
        for (; it != end; ++it) {
          shared_ptr<dynamic_poset::class_range> r = inputs.range(it->first);
          if ( r ) {
            for (dynamic_poset::pset::iterator it = r->first; it != r->second; ++it) {
              shared_ptr<source> s = dynamic_pointer_cast<source>(*it);
              if ( s ) s->init_source();
            }
          } else {
            cout << "invalid source poset class at position " << it->first << endl;
          }
        }
      }

      bool has_inputs() {
        bool found = false;
        dynamic_poset::iterator it = inputs.begin();
        dynamic_poset::iterator end = inputs.end();
        for (; it != end && !found; ++it) {
          shared_ptr<dynamic_poset::class_range> r = inputs.range(it->first);
          if ( r ) {
            for (dynamic_poset::pset::iterator it = r->first;
                 it != r->second && !found; ++it)
            {
              shared_ptr<source> s = dynamic_pointer_cast<source>(*it);
              if ( s ) found = found || s->has_inputs();
            }
          } else {
            cout << "invalid source poset class at position " << it->first << endl;
          }
        }
        return found;
      }

      shared_ptr<list<stream_event> > next_inputs()
      {
        shared_ptr<list<stream_event> > r;
        // pick a random stream until we find one that's not done,
        // and process its frame.
        while ( !current || remaining <= 0 ) {
          if ( inputs.order() < current_order ) {
            cout << "non-monotonic source ordering "
                 << inputs.order() << " vs " << current_order << endl;
            break;
          }

          current_order = inputs.order();
          dynamic_poset::iterator it = inputs.find(current_order);
          if ( it->second ) {
            size_t id = (size_t) (it->second->size() * (rand() / (RAND_MAX + 1.0)));
            dynamic_poset::pset::iterator c_it = it->second->begin();
            advance(c_it, id);
            shared_ptr<source> c = dynamic_pointer_cast<source>(*c_it);
            if ( !c || (c && !c->has_inputs()) ) {
              it->second->erase(c_it);
            } else {
              current = c;
              remaining = (int) (step > 0? step : block*(rand() / (RAND_MAX + 1.0)));
            }
          } else {
            cout << "invalid poset class at position " << it->first << endl;
          }
        }

        if ( !current ) return r;

        r = current->next_inputs();
        if ( r ) remaining -= r->size();
        inputs.reorder_elements(current_order);

        // remove the stream if its done.
        if ( !current->has_inputs() ) {
          remove_source(current);
          current = shared_ptr<source>();
          remaining = 0;
          cout << "done with stream, " << inputs.size() << " remain" << endl;
        }

        return r;
      }
    };

    struct dbt_trigger_log {
      typedef stream<file_sink> file_stream;
      stream_id id;
      shared_ptr<file_stream> sink_stream;
      bool log_stream_name, log_event_type;

      dbt_trigger_log(const path& fp,
                      stream_id s = -1, bool ln = false, bool le = false)
        : id(s), log_stream_name(ln), log_event_type(le)
      {
          init_sink(fp);
      }

      bool has_sink() { return sink_stream; }

      ostream& log_event(string stream_name, stream_event_type t) {
        return
          ( log_stream_name?
            ( log_event_type ?
               ((*sink_stream) << stream_name << "," << t << ",") :
                (*sink_stream) << stream_name << "," )
          : ( log_event_type ? ((*sink_stream) << t << ",") : (*sink_stream) ));
      }

    private:
      void init_sink(const path& fp) {
        sink_stream = shared_ptr<file_stream>(new file_stream(fp.c_str()));
        if ( !sink_stream ) {
           cerr << "failed to open file path " << fp << endl;
        } else if (id < 0) {
            cout << "logging to " << fp << endl;
        } else {
            cout << "logging stream " << id << " to " << fp << endl;
        }
      }
    };

    struct stream_dispatcher {
      typedef boost::function<
        void (const event_data&, shared_ptr<dbt_trigger_log>)> trigger;

      typedef pair<stream_id, stream_event_type> trigger_id;
      typedef map<trigger_id, trigger> dispatch_table;
      typedef map<trigger_id, shared_ptr<dbt_trigger_log> > logger_table;

      shared_ptr<dispatch_table> triggers;
      shared_ptr<logger_table> loggers;
      
      unsigned int log_count_every;
      unsigned int tuple_count;

      stream_dispatcher() : log_count_every(0), tuple_count(0) {
        triggers = shared_ptr<dispatch_table>(new dispatch_table());
        loggers = shared_ptr<logger_table>(new logger_table());
      }
      
      void set_log_count_every(unsigned int _log_count_every){
        log_count_every = _log_count_every;
      }

      bool has_triggers() { return triggers && triggers->size() > 0; }

      void add_trigger(stream_id s, stream_event_type t, trigger tr) {
        trigger_id k = make_pair(s, t);
        if ( triggers->find(k) == triggers->end() )
          (*triggers)[k] = tr;
        else {
          cerr << "Found existing " << (t == insert_tuple? "insert" : "delete")
               << " trigger for stream " << s << endl;
        }
      }

      void remove_trigger(stream_id s, stream_event_type t) {
        trigger_id k = make_pair(s, t);
        dispatch_table::iterator tr_it = triggers->find(k);
        if ( tr_it != triggers->end() ) {
          triggers->erase(tr_it);
          loggers->erase(k);
        }
        else {
          cerr << "Could not find " << (t == insert_tuple? "insert" : "delete")
               << " handler for stream " << s << endl;
        }
      }

      void add_logger(stream_id s, stream_event_type t, const path& fp) {
        trigger_id k = make_pair(s, t);
        shared_ptr<dbt_trigger_log> log =
          shared_ptr<dbt_trigger_log>(new dbt_trigger_log(fp,s));
        add_logger(k, log);
      }

      // Add a logger as both an insert and delete logger.
      void add_logger(stream_id s, const path& fp) {
        shared_ptr<dbt_trigger_log> log =
          shared_ptr<dbt_trigger_log>(new dbt_trigger_log(fp, s, false, true));

        trigger_id k = make_pair(s, insert_tuple);
        add_logger(k, log);

        k = make_pair(s, delete_tuple);
        add_logger(k, log);
      }

      // Add a singe logger for all streams.
      // Requires all triggers to be added beforehand.
      void add_logger(const path& fp) {
        shared_ptr<dbt_trigger_log> log =
          shared_ptr<dbt_trigger_log>(new dbt_trigger_log(fp, -1, true, true));

        dispatch_table::iterator tr_it = triggers->begin();
        for (; tr_it != triggers->end(); ++tr_it) {
          add_logger(tr_it->first, log);
        }
      }

      void remove_logger(stream_id s, stream_event_type t) {
        trigger_id k = make_pair(s, t);
        logger_table::iterator log_it = loggers->find(k);
        if ( log_it != loggers->end() ) loggers->erase(log_it);
      }

      void dispatch(const stream_event& tuple) {
        trigger_id k = make_pair(tuple.id, tuple.type);
        dispatch_table::iterator tr_it = triggers->find(k);
        if ( tr_it != triggers->end() ) {
          logger_table::iterator log_it = loggers->find(k);
          shared_ptr<dbt_trigger_log> logger = log_it == loggers->end()?
            shared_ptr<dbt_trigger_log>() : log_it->second;

          (tr_it->second)(tuple.data, logger);
        } else {
          cerr << "Could not find "
               << (tuple.type == insert_tuple? "insert" : "delete")
               << " handler for stream " << tuple.id << endl;
        }
        if(log_count_every && (tuple_count % log_count_every == 0)){
          struct timeval tp;
          gettimeofday(&tp, NULL);
          cout << tuple_count << " tuples processed at "
               << tp.tv_sec << "s+" << tp.tv_usec << "us" << endl;
        }
        tuple_count += 1;
      }

      private:
      void add_logger(trigger_id id, shared_ptr<dbt_trigger_log> log) {
        dispatch_table::iterator tr_it = triggers->find(id);
        if ( tr_it != triggers->end() ) (*loggers)[id] = log;
      }

    };

    struct stream_registry {
      shared_ptr<stream_multiplexer> multiplexer;
      map<string, shared_ptr<source> > data_sources;
      map<string, list<shared_ptr<stream_adaptor> > > source_adaptors;

      stream_registry(shared_ptr<stream_multiplexer> m) : multiplexer(m) {}

      void register_adaptor(string source_name, shared_ptr<stream_adaptor> a) {
        source_adaptors[source_name].push_back(a);
      }

      void register_source(string name, shared_ptr<source> src) {
        if (data_sources.find(name) != data_sources.end()) {
          cout << "Re-registering source \"" << name << "\"" << endl;
        }
        data_sources[name] = src;
        if ( multiplexer ) { multiplexer->add_source(src); }
      }

      shared_ptr<dbt_file_source> initialize_file_source(
          string stream_name, string file_name, frame_descriptor& f)
      {
        shared_ptr<dbt_file_source> s;
        if ( source_adaptors.find(stream_name) != source_adaptors.end() ) {
          s = shared_ptr<dbt_file_source>(new dbt_file_source(
                stream_name, f, source_adaptors[stream_name]));
          register_source(stream_name, s);
        }
        return s;
      }

      void register_multiplexer(shared_ptr<stream_multiplexer> m) {
        multiplexer = m;
        if ( data_sources.size() > 0 ) {
          map<string, shared_ptr<source> >::iterator src_it = data_sources.begin();
          for (; src_it != data_sources.end(); ++src_it) {
            multiplexer->add_source(src_it->second);
          }
        }
      }
    };
  }
}

#endif

