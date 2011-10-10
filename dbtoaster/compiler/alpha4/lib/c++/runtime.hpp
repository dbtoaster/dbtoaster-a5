#ifndef DBTOASTER_RUNTIME_H
#define DBTOASTER_RUNTIME_H

#include <algorithm>
#include <list>
#include <sstream>
#include <string>
#include <vector>

#include <tr1/unordered_set>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/shared_ptr.hpp>

#include "streams.hpp"

namespace dbtoaster {
  namespace runtime {

    using namespace std;
    using namespace tr1;
    using namespace boost;
    using namespace boost::filesystem;
    using namespace boost::program_options;
    using namespace dbtoaster::streams;
    
    boost::hash<std::string> string_hash;

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

      void init_options(options_description& desc) {
        desc.add_options()
          ("help", "list available options")
          ("log-dir", value<string>(), "logging directory")
          ("log-triggers,l",
            value<vector<string> >(&logged_streams), "log stream triggers")
          ("unified,u", value<string>(), "unified logging [stream | global]")
          ("output-file,o", value<string>(), "output file")
          ("maps,m", value<vector<string> >(&output_maps), "output maps")

          // Statistics profiling parameters
          ("samplesize", value<unsigned int>(),
               "sample window size for trigger profiles")

          ("sampleperiod", value<unsigned int>(),
               "period length, as number of trigger events")

          ("statsfile", value<string>(),
               "output file for trigger profile statistics")

          // Tracing parameters
          ("trace-dir", value<string>(), "trace output dir")
          ("trace,t", value<string>(&trace_opts), "trace query execution")
          ("trace-step,s", value(&trace_step), "trace step size");
      }

      void init_positional_options(positional_options_description& p) {
          p.add("maps", -1);
      }

      void process_options(int argc, char* argv[],
                           options_description& o,
                           positional_options_description& p,
                           variables_map& m)
      {
          try {
            store(command_line_parser(argc,argv).
              options(o).positional(p).run(), m);
            notify(m);
          } catch (unknown_option& o) {
              cout << "unknown option: \"" << o.get_option_name() << "\"" << endl;
              cout << *opt_desc << endl;
              exit(1);
          } catch (error& e) {
              cout << "error parsing command line options" << endl;
              cout << *opt_desc << endl;
              exit(1);
          }
      }

      void setup_tracing(options_description& o) {
        try {
          trace_counter = 0;
          if ( trace_step <= 0 ) trace_step = 1000;
          cout << "tracing: " << trace_opts << endl;
          if ( trace_opts != "" ) parse_tracing(trace_opts);
          else traced = false;
        } catch (unknown_option& uo) {
            cout << "unknown option: \"" << uo.get_option_name() << "\"" << endl;
            cout << o << endl;
            exit(1);
        } catch (error& e) {
            cout << "error parsing command line options" << endl;
            cout << o << endl;
            exit(1);
        }
      }

      void init(int argc, char* argv[]) {
        opt_desc = shared_ptr<options_description>(
            new options_description("dbtoaster query options"));

        init_options(*opt_desc);
        init_positional_options(pos_options);
        process_options(argc, argv, *opt_desc, pos_options, opt_map);
        setup_tracing(*opt_desc);
      }
      
      bool help() {
        if ( opt_map.count("help") ) cout << *opt_desc << endl;
        return opt_map.count("help");
      }

      // Result output.
      string get_output_file() {
        if(opt_map.count("output-file")) {
          return opt_map["output-file"].as<string>();
        } else {
          return string("-");
        }
      }

      bool is_output_map(string map_name) {
        return find(output_maps.begin(), output_maps.end(), map_name)
                != output_maps.end();
      }

      void add_output_map(string map_name){
        output_maps.push_back(map_name);
      }

      // Trigger logging.
      bool global() {
        return opt_map.count("unified")
                 && opt_map["unified"].as<string>() == "global";
      }

      bool unified() {
        return opt_map.count("unified")
                 && opt_map["unified"].as<string>() == "stream";
      }

      path get_log_file(string stream_name, stream_event_type t) {
        string ftype = (t == insert_tuple? "Insert" : "Delete");
        return get_log_file(stream_name, ftype, true);
      }

      path get_log_file(string stream_name) {
        return get_log_file(stream_name, "Events", false);
      }

      void log_dispatcher(stream_dispatcher& d, map<string, int>& stream_ids) {
        if ( global() ) {
          path global_file = get_log_file("", "Events", true);
          d.add_logger(global_file);
        } else {
          vector<string>::iterator it = logged_streams.begin();
          for (; it != logged_streams.end(); ++it) {
            if ( stream_ids.find(*it) != stream_ids.end() ) {
              int id = stream_ids[*it];
              if ( unified() ) d.add_logger(id, get_log_file(*it));
              else {
                d.add_logger(id, insert_tuple, get_log_file(*it, insert_tuple));
                d.add_logger(id, delete_tuple, get_log_file(*it, delete_tuple));
              }
            }
          }
        }
      }

      // Statistics
      // Number of samples to collect per statitics period.
      unsigned int get_stats_window_size() {
        unsigned int r = 10000;
        if ( opt_map.count("samplesize") ) {
          r = opt_map["samplesize"].as<unsigned int>();
        }
        return r;
      }

      // Period size, in terms of the number of trigger invocations.
      unsigned int get_stats_period() {
        unsigned int r = 10000;
        if ( opt_map.count("sampleperiod") ) {
          r = opt_map["sampleperiod"].as<unsigned int>();
        }
        return r;
      }

      string get_stats_file() {
        string r = "stats";
        if ( opt_map.count("statsfile") ) {
          r = opt_map["statsfile"].as<string>();
        }
        return r;
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

      private:
      path get_log_file(string stream_name, string ftype, bool prefix) {
        path r;
        if ( opt_map.count("log-dir") ) r = opt_map["log-dir"].as<string>();
        else r = current_path();
        r /= (prefix? ftype : "") + stream_name + (prefix? "" : ftype) + ".dbtdat";
        return r.make_preferred();
      }
    };

    struct orderbook_options : public runtime_options {
      vector<string> orderbook_params;
      orderbook_options() {}
      orderbook_options(int argc, char* argv[]) { init(argc, argv); }

      void init(int argc, char* argv[]) {
        runtime_options::init_options(*opt_desc);

        // Additional
        opt_desc->add_options()
          ("input-file,i", value<string>(), "order book input data file")
          ("orderbook-params", value<vector<string> >(&orderbook_params),
              "order book adaptor parameters");

        // No positional parameters.
        runtime_options::process_options(argc, argv, *opt_desc, pos_options, opt_map);
        runtime_options::setup_tracing(*opt_desc);
      }

      string order_book_file() {
        string r;
        if ( opt_map.count("input-file") ) {
          r = opt_map["input-file"].as<string>();
        }
        return r;
      }

      string order_book_params() {
        string r;
        vector<string>::iterator it = orderbook_params.begin();
        for (; it != orderbook_params.end(); ++it)
          r += (r.empty()? "" : ",") + *it;
        return r;
      }
    };
  }
}

#endif
