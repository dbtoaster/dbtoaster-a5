#ifndef DBTOASTER_RUNTIME_H
#define DBTOASTER_RUNTIME_H

#include <algorithm>
#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <tr1/unordered_set>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/any.hpp>
#include <boost/filesystem.hpp>
#include <boost/function.hpp>
#include <boost/lambda/lambda.hpp>
#include <boost/lambda/bind.hpp>
#include <boost/program_options.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/file.hpp>

#include "streams.hpp"

namespace dbtoaster {
  namespace runtime {

    using namespace std;
    using namespace tr1;
    using namespace boost;
    using namespace boost::filesystem;
    using namespace boost::iostreams;
    using namespace boost::lambda;
    using namespace boost::program_options;


    // Adaptor and stream interfaces.
    struct stream_adaptor
    {
      // processes the data, adding all stream events generated to the list.
      virtual void process(const string& data,
                           shared_ptr<list<stream_event> > dest) = 0;
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

    // Sources
    struct source
    {
      frame_descriptor frame_info;
      list<shared_ptr<stream_adaptor> > adaptors;

      source(frame_descriptor& f, list<shared_ptr<stream_adaptor> >& a)
        : frame_info(f), adaptors(a)
      {}

      virtual void init_source() = 0;
      virtual bool has_inputs() = 0;
      virtual shared_ptr<list<stream_event> > next_inputs() = 0;
    };

    struct dbt_file_source : public source
    {
      typedef stream<file_source> file_stream;
      shared_ptr<file_stream> source_stream;
      shared_ptr<string> buffer;
      dbt_file_source(const string& path, frame_descriptor& f,
                      list<shared_ptr<stream_adaptor> >& a)
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

      shared_ptr<list<stream_event> > next_inputs() {
        shared_ptr<list<stream_event> > r;
        if ( has_inputs() ) {
          // get the next frame of data based on the frame type.
          shared_ptr<string> data = next_frame();

          if ( data ) {
            r = shared_ptr<list<stream_event> >(new list<stream_event>());
            // process all adaptors, accumulating and returning stream events
            for_each(adaptors.begin(), adaptors.end(),
                bind(&stream_adaptor::process, *_1, *data, r));
          }
        } else if ( source_stream->is_open() ) {
            source_stream->close();
        }
        return r;
      }
    };

    struct stream_multiplexer
    {
      set< shared_ptr<source> > inputs;
      shared_ptr<source> current;
      int step, remaining;

      stream_multiplexer(int seed, int st) : step(st), remaining(0) {
        srandom(seed);
      }

      stream_multiplexer(int seed, int st, set<shared_ptr<source> >& s) : inputs(s)
      {
        stream_multiplexer(seed, st);
      }

      void add_source(shared_ptr<source> s) { inputs.insert(s); }

      void remove_source(shared_ptr<source> s) { inputs.erase(s); }

      void init_source() {
        for_each(inputs.begin(), inputs.end(), bind(&source::init_source, *_1));
      }

      bool has_inputs() {
        return inputs.end() !=
          find_if(inputs.begin(), inputs.end(), bind(&source::has_inputs, *_1));
      }

      shared_ptr<list<stream_event> > next_inputs() {
        shared_ptr<list<stream_event> > r;
        // pick a random stream until we find one that's not done,
        // and process its frame.
        while ( !current || remaining == 0 ) {
          size_t id = (size_t) (inputs.size() * (rand() / (RAND_MAX + 1.0)));
          set<shared_ptr<source> >::iterator it = inputs.begin();
          advance(it, id);
          if ( !(*it)->has_inputs() ) inputs.erase(it);
          else {
            current = *it;
            remaining = (int) (step > 0? step : (rand() / (RAND_MAX + 1.0)));
          }
        }

        r = current->next_inputs();
        if ( r ) remaining -= r->size();

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
      stream_event_type type;
      stream_id id;
      shared_ptr<file_stream> sink_stream;

      dbt_trigger_log(stream_id i, stream_event_type t, const path& fp)
        : id(i), type(t)
      {
        sink_stream = shared_ptr<file_stream>(new file_stream(fp.c_str()));
        if ( !sink_stream ) {
          cerr << "failed to open file path " << fp << endl;
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

      stream_dispatcher() {
        triggers = shared_ptr<dispatch_table>(new dispatch_table());
        loggers = shared_ptr<logger_table>(new logger_table());
      }

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
        dispatch_table::iterator tr_it = triggers->find(k);
        if ( tr_it != triggers->end() ) {
          (*loggers)[k] =
             shared_ptr<dbt_trigger_log>(new dbt_trigger_log(s,t,fp));
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
      }
    };

    struct runtime_options {
      shared_ptr<options_description> opt_desc;
      variables_map opt_map;
      positional_options_description pos_options;

      vector<string> output_maps;
      vector<string> logged_streams;

      // Tracing
      string trace_opts;
      bool traced;
      int trace_counter, trace_step;
      unordered_set<string> traced_maps;

      runtime_options() { traced = false; trace_step = trace_counter = 0; }

      runtime_options(int argc, char* argv[]) { init(argc, argv); }

      void init(int argc, char* argv[]) {
        opt_desc = shared_ptr<options_description>(
            new options_description("dbtoaster query options"));
        opt_desc->add_options()
          ("help", "list available options")
          ("log-dir", value<string>(), "logging directory")
          ("log-triggers,l", value<vector<string> >(&logged_streams), "log stream triggers")
          ("output-file,o", value<string>(), "output file")
          ("maps,m", value<vector<string> >(&output_maps), "output maps")
          ("trace-dir", value<string>(), "trace output dir")
          ("trace,t", value<string>(&trace_opts), "trace query execution")
          ("trace-step,s", value(&trace_step), "trace step size");
        pos_options.add("maps", -1);

        store(command_line_parser(argc,argv).
          options(*opt_desc).positional(pos_options).run(), opt_map);
        notify(opt_map);

        trace_counter = 0;
        if ( trace_step <= 0 ) trace_step = 1000;
        cout << "tracing: " << trace_opts << endl;
        if ( trace_opts != "" ) parse_tracing(trace_opts);
        else traced = false;
      }

      bool help() {
        if ( opt_map.count("help") ) cout << *opt_desc << endl;
        return opt_map.count("help");
      }

      // Result output.
      string get_output_file() {
        string r = "maps.txt";
        if ( opt_map.count("output-file") )
            r = opt_map["output-file"].as<string>();
        return r;
      }

      bool is_output_map(string map_name) {
        return find(output_maps.begin(), output_maps.end(), map_name)
                != output_maps.end();
      }

      // Trigger logging.
      path get_log_file(string stream_name, stream_event_type t) {
        path r;
        if ( opt_map.count("log-dir") ) r = opt_map["log-dir"].as<string>();
        else r = current_path();
        r /= stream_name + (t == insert_tuple? "inserts" : "deletes") + ".txt";
        return r.make_preferred();
      }

      void log_dispatcher(stream_dispatcher& d, map<string, int>& stream_ids) {
        vector<string>::iterator it = logged_streams.begin();
        for (; it != logged_streams.end(); ++it) {
          if ( stream_ids.find(*it) != stream_ids.end() ) {
            d.add_logger(stream_ids[*it], insert_tuple, get_log_file(*it, insert_tuple));
            d.add_logger(stream_ids[*it], delete_tuple, get_log_file(*it, delete_tuple));
          }
        }
      }

      // Tracing.
      void parse_tracing(const string& opts) {
        split_iterator<string::const_iterator> it =
           make_split_iterator(opts, first_finder(",", is_equal()));

        split_iterator<string::const_iterator> end;

        for (; it != end; ++it) {
          string param = copy_range<std::string>(*it);
          cout << "tracing map " << param << endl;
          traced_maps.insert(param);
        }
        traced = true;
      }

      bool is_traced_map(string map_name) {
        return traced_maps.empty()
                || (traced_maps.find(map_name) != traced_maps.end());
      }

      bool is_traced() {
        ++trace_counter;
        return traced && (trace_counter % trace_step) == 0;
      }

      path get_trace_file() {
        path p = "traces";
        if ( opt_map.count("trace-dir") ) {
          p = opt_map["trace-dir"].as<string>();
        }
        p /= "trace"+boost::lexical_cast<std::string>(trace_counter)+".txt";
        cout << "trace file " << p << endl;
        return p.make_preferred();
      }
    };
  }
}

#endif
