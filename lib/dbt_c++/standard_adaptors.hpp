#ifndef DBTOASTER_STDADAPTORS_H
#define DBTOASTER_STDADAPTORS_H

#include <string>
#include <list>
#include <map>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/functional/hash.hpp>
#include <boost/shared_ptr.hpp>

#include "streams.hpp"

namespace dbtoaster {
  namespace adaptors {

    using namespace std;
    using namespace dbtoaster::streams;

    struct csv_adaptor : public stream_adaptor
    {
      stream_id_t id;
      stream_event_type type;
      string schema;
      string delimiter;
      boost::hash<std::string> field_hash;

      shared_ptr<stream_event> saved_event;

      csv_adaptor(stream_id_t i) : id(i), type(insert_tuple), delimiter(",") {}

      csv_adaptor(stream_id_t i, string sch)
        : id(i), type(insert_tuple), schema(sch), delimiter(",")
      {
        validate_schema();
      }

      csv_adaptor(stream_id_t i, int num_params,
                  const pair<string,string> params[])
        : id(i), type(insert_tuple), delimiter(",")
      {
        parse_params(num_params,params);
        validate_schema();
      }

      void parse_params(int num_params, const pair<string, string> params[]) {
        for (int i = 0; i< num_params; ++i) {
          string k = params[i].first;
          string v = params[i].second;
          if( __verbose )
        	  cerr << "csv params: " << k << ": " << v << endl;

          if ( k == "fields" ) {
            delimiter = v;
          } else if ( k == "schema" ) {
            schema = parse_schema(v);
          } else if ( k == "eventtype" ) {
             type = v == "insert"? insert_tuple : delete_tuple;
          }
          // TODO: handle parameterized events, via 'triggers' key as seen
          // in OCaml adaptors.
        }
      }

      virtual string parse_schema(string s)
      {
        string r = "";
        split_iterator<string::iterator> end;
        for (split_iterator<string::iterator> it =
             make_split_iterator(s, first_finder(",", is_equal()));
             it != end; ++it)
        {
          string ty = copy_range<std::string>(*it);
          if ( ty == "event" )      r += "e";
          else if ( ty == "order" ) r += "o";
          else if ( ty == "int" )   r += "i";
          else if ( ty == "long" )  r += "l";
          else if ( ty == "float" ) r += "f";
          else if ( ty == "date" )  r += "d";
          else if ( ty == "hash" )  r += "h";
          else {
            cerr << "invalid csv schema type " << ty << endl;
            r = "";
            break;
          }
        }
        return r;
      }

      void validate_schema() {
        bool valid = true;
        string::iterator it = schema.begin();
        for (; valid && it != schema.end(); ++it) {
          switch(*it) {
            case 'e':  // event type
            case 'o':  // order field type
            case 'i':
            case 'l':
            case 'f':
            case 'd':
            case 'h': break;
            default: valid = false; break;
          }
        }
        if ( !valid ) schema = "";
      }

      // Interpret the schema.
      tuple<bool, bool, event_args> interpret_event(const string& schema,
                                                    const string& data)
      {
        event_args tuple;
        string::const_iterator schema_it = schema.begin();
        bool valid = true;

        // Default to the adaptor's event type, and override with an event
        // field in the schema.
        bool insert = type == insert_tuple;

        split_iterator<string::const_iterator> field_end;
        for (split_iterator<string::const_iterator> field_it =
                make_split_iterator(data, first_finder(delimiter, is_equal()));
             valid && schema_it != schema.end() && field_it != field_end;
             ++schema_it, ++field_it)
        {
          string field = copy_range<std::string>(*field_it);
          istringstream iss(field);
          bool ins; unsigned int o;
          int y,m,d;
          double f; int i; long l;
          vector<string> date_fields;
          switch (*schema_it) {
            case 'e': iss >> ins; insert = ins; break;
            case 'i': iss >> i; tuple.push_back(i); break;
            case 'l': iss >> l; tuple.push_back(l); break;
            case 'f': iss >> f; tuple.push_back(f); break;
            case 'h': tuple.push_back(static_cast<int>(field_hash(field))); break;
            case 'd':
              split(date_fields, field, is_any_of("-"));
              valid = false;
              if ( date_fields.size() == 3 ) {
                y = atoi(date_fields[0].c_str());
                m = atoi(date_fields[1].c_str());
                d = atoi(date_fields[2].c_str());
                if ( 0 < m && m < 13 && 0 < d && d <= 31) {
                  tuple.push_back(y*10000+m*100+d); valid = true;
                }
              }
              break;
            case 'o':
                iss >> o;
                if ( o < current_order ) {
                  cerr << "invalid adaptor order " << o
                       << " (current " << current_order << ")" << endl;
                } else {
                  current_order = o;
                }
                break;
            default: valid = false; break;
          }
          valid = valid && !iss.fail();
        }
        return make_tuple(valid, insert, tuple);
      }

      void process(const string& data, shared_ptr<list<stream_event> > dest)
      {
        // Flush any buffered tuple.
        if ( saved_event ) {
            dest->push_back(*saved_event);
            saved_event = shared_ptr<stream_event>();
        }

        if ( dest && schema != "" ) {
          // Interpret the schema.
          unsigned int order_before = current_order;
          tuple<bool, bool, event_args> evt = interpret_event(schema, data);
          bool valid = get<0>(evt);
          bool insert = get<1>(evt);

          if ( valid )  {
            stream_event e(insert? insert_tuple : delete_tuple, id, get<2>(evt));
            // Buffer on change of order.
            if ( current_order > order_before ) {
              saved_event = shared_ptr<stream_event>(new stream_event(e));
            } else {
              dest->push_back(e);
            }
          } else {
            cerr << "adaptor could not process " << data << endl;
            cerr << "schema: " << schema << endl;
          }
        } else if ( __verbose ) {
           cerr << "Skipping event, no "
                << (schema == ""? "schema" : "buffer") << " found." << endl;
        }
      }

      void finalize(shared_ptr<list<stream_event> > dest) {}
    };

    // Replay adaptors are CSV adaptors prepended with an integer denoting the
    // event type. The adaptor internally adjusts the schema, allowing it to
    // be used with the same parameters as a standard CSV adaptor.
    struct replay_adaptor : public csv_adaptor {
      replay_adaptor(stream_id_t i) : csv_adaptor(i) {}

      replay_adaptor(stream_id_t i, string sch) : csv_adaptor(i) {
        type = insert_tuple;
        schema = "e,"+sch;
        validate_schema();
      }

      replay_adaptor(stream_id_t i, int num_params,
                     const pair<string,string> params[])
        : csv_adaptor(i,num_params,params)
      {}

      string parse_schema(string s) {
        return csv_adaptor::parse_schema("event,"+s);
      }
    };
  }

  namespace datasets {

    //////////////////////////////
    //
    // Order books

    namespace order_books
    {
      using namespace dbtoaster::adaptors;

      enum order_book_type { tbids, tasks, both };

      // Struct to represent messages coming off a socket/historical file
      struct order_book_message {
          double t;
          int id;
          string action;
          double volume;
          double price;
      };

      // Struct for internal storage, i.e. a message without the action or
      // order id.
      struct order_book_tuple {
          double t;
          int id;
          int broker_id;
          double volume;
          double price;
          order_book_tuple() {}

          order_book_tuple(const order_book_message& msg) {
            t = msg.t;
            id = msg.id;
            volume = msg.volume;
            price = msg.price;
          }

          order_book_tuple& operator=(order_book_tuple& other) {
            t = other.t;
            id = other.id;
            broker_id = other.broker_id;
            volume = other.volume;
            price = other.price;
            return *this;
          }

          void operator()(event_args& e) {
            if (e.size() > 0) e[0] = t; else e.push_back(t);
            if (e.size() > 1) e[1] = id; else e.push_back(id);
            if (e.size() > 2) e[2] = broker_id; else e.push_back(broker_id);
            if (e.size() > 3) e[3] = volume; else e.push_back(volume);
            if (e.size() > 4) e[4] = price; else e.push_back(price);
          }
      };

      typedef map<int, order_book_tuple> order_book;

      struct order_book_adaptor : public stream_adaptor {
        stream_id_t id;
        int num_brokers;
        order_book_type type;
        shared_ptr<order_book> bids;
        shared_ptr<order_book> asks;
        bool deterministic;
        bool insert_only;


        order_book_adaptor(stream_id_t sid, int nb, order_book_type t)
          : id(sid), num_brokers(nb), type(t)
        {
          bids = shared_ptr<order_book>(new order_book());
          asks = shared_ptr<order_book>(new order_book());
          deterministic = false;
          insert_only = false;
        }

        order_book_adaptor(stream_id_t sid, int num_params,
                           pair<string, string> params[])
        {
          id = sid;
          bids = shared_ptr<order_book>(new order_book());
          asks = shared_ptr<order_book>(new order_book());
          deterministic = false;
          num_brokers = 10;

          for (int i = 0; i < num_params; ++i) {
            string k = params[i].first;
            string v = params[i].second;
            if( __verbose )
            	cerr << "order book adaptor params: "
            		<< params[i].first << ", " << params[i].second << endl;

            if ( k == "book" ) {
              type = (v == "bids"? tbids : tasks);
            } else if ( k == "brokers" ) {
              num_brokers = atoi(v.c_str());
            } else if ( k == "validate" ) { // Ignore.
            } else if ( k == "deterministic" ) {
              deterministic = (v == "yes");
            } else if ( k == "insert-only" ) {
              insert_only = true;
            } else {
              cerr << "Invalid order book param " << k << ", " << v << endl;
            }
          }
        }

        bool parse_error(const string& data, int field) {
          cerr << "Invalid field " << field << " message " << data << endl;
          return false;
        }

        // Expected message format: t, id, action, volume, price
        bool parse_message(const string& data, order_book_message& r) {
          string msg = data;
          char* start = &(msg[0]);
          char* end = start;
          char action;

          for (int i = 0; i < 5; ++i)
          {
              while ( *end && *end != ',' ) ++end;
              if ( start == end ) { return parse_error(data, i); }
              if ( *end == '\0' && i != 4 ) { return parse_error(data, i); }
              *end = '\0';

              switch (i) {
              case 0: r.t = atof(start); break;
              case 1: r.id = atoi(start); break;
              case 2:
                  action = *start;
                  if ( !(action == 'B' || action == 'S' ||
                         action == 'E' || action == 'F' ||
                         action == 'D' || action == 'X' ||
                         action == 'C' || action == 'T') )
                  {
                     return parse_error(data, i);
                  }

                  r.action = action;
                  break;

              case 3: r.volume = atof(start); break;
              case 4: r.price = atof(start); break;
              default: return parse_error(data, i);
              }

              start = ++end;
          }
          return true;
        }

        void process_message(const order_book_message& msg,
                             shared_ptr<list<stream_event> > dest)
        {
            bool valid = true;
            order_book_tuple r(msg);
            stream_event_type t = insert_tuple;

            if ( msg.action == "B" ) {
              if (type == tbids || type == both) {
                r.broker_id = 
                  (deterministic ? msg.id : ((int) rand())) % num_brokers;
                (*bids)[msg.id] = r;
                t = insert_tuple;
              } else valid = false;
            }
            else if ( msg.action == "S" ) {
              if (type == tasks || type == both) {
                r.broker_id = 
                  (deterministic ? msg.id : ((int) rand())) % num_brokers;
                (*asks)[msg.id] = r;
                t = insert_tuple;
              } else valid = false;
            }

            else if ( msg.action == "E" ) {
              order_book_tuple x;
              bool x_valid = true;
              order_book::iterator bid_it = bids->find(msg.id);
              if ( bid_it != bids->end() ) {
                x = r = bid_it->second;
                r.volume -= msg.volume;
                if ( r.volume <= 0.0 ) { bids->erase(bid_it); valid = false; }
                else { (*bids)[msg.id] = r; }
              } else {
                order_book::iterator ask_it = asks->find(msg.id);
                if ( ask_it != asks->end() ) {
                  x = r = ask_it->second;
                  r.volume -= msg.volume;
                  if ( r.volume <= 0.0 ) { asks->erase(ask_it); valid = false; }
                  else { (*asks)[msg.id] = r; }
                } else {
                  //cerr << "unknown order id " << msg.id
                  //     << " (neither bid nor ask)" << endl;
                  valid = false;
                  x_valid = false;
                }
              }
              if ( x_valid && !insert_only ) {
                event_args fields(5);
                x(fields);
                stream_event y(delete_tuple, id, fields);
                dest->push_back(y);
              }
              t = insert_tuple;
            }

            else if ( msg.action == "F" )
            {
              order_book::iterator bid_it = bids->find(msg.id);
              if ( bid_it != bids->end() ) {
                r = bid_it->second;
                bids->erase(bid_it);
              } else {
                order_book::iterator ask_it = asks->find(msg.id);
                if ( ask_it != asks->end() ) {
                  r = ask_it->second;
                  asks->erase(ask_it);
                } else {
                  //cerr << "unknown order id " << msg.id
                  //     << " (neither bid nor ask)" << endl;
                  valid = false;
                }
              }
              t = delete_tuple;
            }

            else if ( msg.action == "D" )
            {
              order_book::iterator bid_it = bids->find(msg.id);
              if ( bid_it != bids->end() ) {
                r = bid_it->second;
                bids->erase(bid_it);
              } else {
                order_book::iterator ask_it = asks->find(msg.id);
                if ( ask_it != asks->end() ) {
                  r = ask_it->second;
                  asks->erase(ask_it);
                } else {
                  //cerr << "unknown order id " << msg.id
                  //     << " (neither bid nor ask)" << endl;
                  valid = false;
                }
              }
              t = delete_tuple;
            }

            /*
            // ignore for now...
            else if ( v->action == "X")
            else if ( v->action == "C")
            else if ( v->action == "T")
            */
            else { valid = false; }


            if ( valid ) {
              event_args fields(5);
              r(fields);
              if ( !(t == delete_tuple && insert_only) ) {
                stream_event e(t, id, fields);
                dest->push_back(e);
              }
            }
        }

        void process(const string& data, shared_ptr<list<stream_event> > dest)
        {
            // Grab a message from the data.
            order_book_message r;
            bool valid = parse_message(data, r);

            if ( valid ) {
              // Process its action, updating the internal book.
              process_message(r, dest);
            }
        }

        void finalize(shared_ptr<list<stream_event> > dest) {}
      };

      // Command line initialization of orderbook datasets.
      struct order_book_streams {
        // stream name, adaptor params
        typedef pair<string, vector<pair<string, string> > > stream_params;
        stream_registry r;
        map<string, int> stream_identifiers;
        int sid;
        string data_file;

        order_book_streams(string file_name, string params,
                           shared_ptr<source_multiplexer> m)
          : r(m), sid(0), data_file(file_name)
        {
          init(parse_params(params));
        }

        vector<stream_params> parse_params(string params) {
          vector<stream_params> r;
          shared_ptr<stream_params> current;

          vector<string> tmp; split(tmp, params, is_any_of(","));
          for (vector<string>::iterator it = tmp.begin(); it != tmp.end(); ++it) {
            string p = *it;
            vector<string> p_parts; split(p_parts, p, is_any_of(":"));
            if ( p_parts.size() == 2 ) {
              if ( p_parts[0] == "book" ) {
                if ( current ) r.push_back(*current);
                current = shared_ptr<stream_params>(new stream_params());
                to_lower(p_parts[1]);
                current->first = p_parts[1];
                current->second.push_back(make_pair(p_parts[0], p_parts[1]));
              } else if ( current ){
                current->second.push_back(make_pair(p_parts[0], p_parts[1]));
              } else {
                cerr << "must declare order book before parameter " << p << endl;
              }
            } else {
              cerr << "invalid order book adaptor parameter " << p << endl;
            }
          }

          return r;
        }

        void init(vector<stream_params> params) {
          if ( !data_file.empty() ) {
            for (int i = 0; i < params.size(); ++i) {
              stream_params& p = params[i];
              frame_descriptor fdesc("\n");
              shared_ptr<order_book_adaptor> a = shared_ptr<order_book_adaptor>(
                new order_book_adaptor(sid, p.second.size(), &(p.second[0])));
              if ( a ) {
                r.register_adaptor(p.first, a);
                shared_ptr<source> s = r.initialize_file_source(p.first, data_file, fdesc);
                if ( s ) {
                  stream_identifiers[p.first] = sid;
                  ++sid;
                } else {
                  cerr << "failed to initialize source for " << p.first << endl;
                }
              } else {
                cerr << "failed to create adaptor for " << p.first << endl;
              }
            }
          } else {
            cerr << "No data file specified for order book streams" << endl;
          }
        }

        map<string, int>& get_stream_identifiers() { return stream_identifiers; }

        int get_stream_id(string name) {
          int r = -1;
          if ( stream_identifiers.find(name) != stream_identifiers.end() )
            r = stream_identifiers[name];
          return r;
        }

        int get_bids_stream_id() { return get_stream_id("bids"); }
        int get_asks_stream_id() { return get_stream_id("asks"); }
      };
    }

    //////////////////////////////
    //
    // TPCH files

    namespace tpch
    {
      using namespace dbtoaster::adaptors;

      struct tpch_adaptor : public csv_adaptor {
        int num_fields;
        bool deletions;

        tpch_adaptor(stream_id_t i, string tpch_rel) : csv_adaptor(i),
                                                     deletions(false){
          schema = parse_schema(get_schema(tpch_rel));
          if ( delimiter == "" ) delimiter = "|";
        }

        tpch_adaptor(stream_id_t i, string tpch_rel, int num_params,
                     const pair<string, string> params[])
          : csv_adaptor(i,num_params,params), deletions(false)
        {
          // Parse TPCH adaptor-specific parameters.
          for (int i = 0; i< num_params; ++i) {
            string k = params[i].first;
            string v = params[i].second;
            if( __verbose )
            	cerr << "tpch adaptor params: " << k << ": " << v << endl;
            if ( k == "deletions" ) {
              deletions = (v == "true");
            }
          }

          schema = parse_schema(get_schema(tpch_rel));
          if ( delimiter == "" ) delimiter = "|";
        }

        string get_schema(string tpch_rel) {
          string r = deletions? "order,event," : "";
          if ( tpch_rel == "lineitem" ) {
            r += "int,int,int,int,int,float,float,float,hash,hash,date,date,date,hash,hash,hash";
            num_fields = 16;
          } else if ( tpch_rel == "orders" ) {
            r += "int,int,hash,float,date,hash,hash,int,hash";
            num_fields = 9;
          } else if ( tpch_rel == "customer" ) {
            r += "int,hash,hash,int,hash,float,hash,hash";
            num_fields = 8;
          } else if ( tpch_rel == "supplier") {
            r += "int,hash,hash,int,hash,float,hash";
            num_fields = 7;
          } else if ( tpch_rel == "part") {
            r += "int,hash,hash,hash,hash,int,hash,float,hash";
            num_fields = 9;
          } else if ( tpch_rel == "partsupp") {
            r += "int,int,int,float,hash";
            num_fields = 5;
          } else if ( tpch_rel == "nation") {
            r += "int,hash,int,hash";
            num_fields = 4;
          } else if ( tpch_rel == "region") {
            r += "int,hash,hash";
            num_fields = 3;
          } else {
            cerr << "Invalid TPCH relation " << tpch_rel << endl;
          }
          return r;
        }
      };

      // TODO: needs testing.
      struct non_inflationary_tpch_adaptor : public tpch_adaptor
      {
        unsigned int gap_counter;
        bool reverse;
        shared_ptr<list<stream_event> > buffer;

        non_inflationary_tpch_adaptor(stream_id_t i, string tpch_rel,
                                      unsigned int gap, bool rev)
          : tpch_adaptor(i,tpch_rel)
        {
          gap_counter = gap > 0? gap : 100;
          reverse = rev;
          buffer = shared_ptr<list<stream_event> >(new list<stream_event>());
        }

        non_inflationary_tpch_adaptor(stream_id_t i, string tpch_rel,
                                      int num_params,
                                      const pair<string, string> params[])
          : tpch_adaptor(i, tpch_rel, num_params, params)
        {
          // Parse adaptor-specific parameters.
          for (int i = 0; i< num_params; ++i) {
            string k = params[i].first;
            string v = params[i].second;
            if ( k == "gap" ) {
              istringstream iss(v); iss >> gap_counter;
              if ( iss.fail() ) {
                cerr << "invalid gap parameter " << v << endl;
                gap_counter = 100;
              }
            } else if ( k == "reverse" ) {
              reverse = (v == "true");
            }
          }
          buffer = shared_ptr<list<stream_event> >(new list<stream_event>());
        }

        void process(const string& data, shared_ptr<list<stream_event> > dest)
        {
          if ( dest && schema != "" ) {
            // Interpret the schema.
            tuple<bool, bool, event_args> evt = interpret_event(schema, data);
            bool valid = get<0>(evt);
            bool insert = get<1>(evt);

            if ( valid )  {
              stream_event e(insert? insert_tuple : delete_tuple, id, get<2>(evt));
              dest->push_back(e);

              if ( !buffer ) return;
              if ( insert ) {
                stream_event de(delete_tuple, id, get<2>(evt));
                if ( reverse ) buffer->push_back(de);
                else buffer->push_front(de);
              }

              if ( (--gap_counter) == 0 && !buffer->empty() ) {
                dest->push_back(buffer->front());
                buffer->pop_front();
              }
            } else {
              cerr << "adaptor could not process " << data << endl;
            }
          }
        }

        void finalize(shared_ptr<list<stream_event> > data) {
          if ( data && buffer && !buffer->empty() ) {
            copy(buffer->begin(), buffer->end(), back_inserter(*data));
          }
        }
      };
    }
  }
}

#endif
